//! Single-shard, multi-node fault-tolerance tests — the Ledge safety proof.
//!
//! Each test spins an in-process Raft cluster over the in-memory network and
//! asserts a consensus safety property: leader election, replication,
//! linearizable CAS, crash failover with no data loss, and snapshot-driven
//! convergence of a lagging node.

use ledge_cluster::testkit::TestCluster;
use ledge_core::ObjectId;
use ledge_raft::{LedgeOp, LedgeResp};
use openraft::ServerState;

/// Build a `RefUpdate` op. `target`/`expected` are byte fills so assertions can
/// reconstruct the expected `ObjectId` cheaply.
fn op_create(name: &str, target: u8, hlc: u64) -> LedgeOp {
    LedgeOp::RefUpdate {
        name: name.to_string(),
        target_bytes: [target; 32],
        expected_bytes: None,
        hlc,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_node_client_write_roundtrip() {
    // Single-member cluster {1}: node 1 elects itself immediately.
    let c = TestCluster::new_single_shard(&[1], None).await;
    c.initialize(1).await;
    let leader = c.wait_for_leader().await;
    assert_eq!(leader, 1);

    let resp = c
        .leader(1)
        .client_write(op_create("refs/heads/main", 0xaa, 1))
        .await
        .expect("client_write ok");
    // VERIFIED 0.9.24: app response is the `.data` field of ClientWriteResponse.
    match resp.data {
        LedgeResp::RefUpdated(e) => assert_eq!(e.target, ObjectId::from_bytes([0xaa; 32])),
        other => panic!("expected RefUpdated, got {other:?}"),
    }
    // And the SM applied it locally.
    let applied = c.node(1).sm.applied_ref("refs/heads/main").await;
    assert_eq!(applied.unwrap().target, ObjectId::from_bytes([0xaa; 32]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn elects_leader() {
    let c = TestCluster::new_single_shard(&[1, 2, 3], None).await;
    c.initialize(1).await;
    // Wait for full convergence (all nodes agree, exactly one leader) rather than
    // the first leader sighting — a follower's view can lag the election by a
    // heartbeat, which made the per-node assertions below racy under CI load.
    let leader = c.wait_for_stable_leader().await;

    // Exactly one node reports Leader; the other two are Followers.
    let mut leaders = 0;
    for id in [1, 2, 3] {
        let m = c.node(id).raft.metrics().borrow().clone();
        if m.state == ServerState::Leader {
            leaders += 1;
            assert_eq!(m.current_leader, Some(id));
        }
        // Every node agrees on who the leader is.
        assert_eq!(
            m.current_leader,
            Some(leader),
            "node {id} disagrees on leader"
        );
    }
    assert_eq!(leaders, 1, "expected exactly one leader");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replicates_writes() {
    let c = TestCluster::new_single_shard(&[1, 2, 3], None).await;
    c.initialize(1).await;
    let leader = c.wait_for_leader().await;

    // Write three refs through the leader.
    for (i, name) in ["refs/heads/a", "refs/heads/b", "refs/heads/c"]
        .iter()
        .enumerate()
    {
        let r = c
            .leader(leader)
            .client_write(op_create(name, 0x10 + i as u8, (i + 1) as u64))
            .await
            .expect("client_write");
        assert!(matches!(r.data, LedgeResp::RefUpdated(_)));
    }

    // Every node's state machine must converge to all three refs.
    // Followers apply asynchronously; poll briefly.
    for id in [1, 2, 3] {
        for (i, name) in ["refs/heads/a", "refs/heads/b", "refs/heads/c"]
            .iter()
            .enumerate()
        {
            let want = ObjectId::from_bytes([0x10 + i as u8; 32]);
            let mut got = None;
            for _ in 0..200 {
                if let Some(e) = c.node(id).sm.applied_ref(name).await {
                    got = Some(e.target);
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            assert_eq!(got, Some(want), "node {id} missing {name}");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cas_linearizable() {
    let c = TestCluster::new_single_shard(&[1, 2, 3], None).await;
    c.initialize(1).await;
    let leader = c.wait_for_leader().await;
    let raft = c.leader(leader).clone();

    // Seed the ref at v_base.
    let base = [0x01u8; 32];
    let seeded = raft
        .client_write(op_create("refs/heads/cas", 0x01, 1))
        .await
        .unwrap();
    assert!(matches!(seeded.data, LedgeResp::RefUpdated(_)));

    // Two contending updates, SAME expected = base, different targets.
    let mk = |t: u8, hlc: u64| LedgeOp::RefUpdate {
        name: "refs/heads/cas".into(),
        target_bytes: [t; 32],
        expected_bytes: Some(base),
        hlc,
    };
    let r1 = raft.clone();
    let r2 = raft.clone();
    let (a, b) = tokio::join!(
        async move { r1.client_write(mk(0xaa, 2)).await.unwrap().data },
        async move { r2.client_write(mk(0xbb, 3)).await.unwrap().data },
    );

    // Exactly one Updated, exactly one Conflict.
    let updated = [&a, &b]
        .iter()
        .filter(|r| matches!(r, LedgeResp::RefUpdated(_)))
        .count();
    let conflict = [&a, &b]
        .iter()
        .filter(|r| matches!(r, LedgeResp::Conflict(_)))
        .count();
    assert_eq!(updated, 1, "expected exactly one winner: a={a:?} b={b:?}");
    assert_eq!(
        conflict, 1,
        "expected exactly one conflict: a={a:?} b={b:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_failure_no_data_loss() {
    let c = TestCluster::new_single_shard(&[1, 2, 3], None).await;
    c.initialize(1).await;
    let old_leader = c.wait_for_leader().await;

    // Commit some refs and confirm they are committed (client_write returns
    // only after the entry is applied on the leader, i.e. committed).
    for (i, name) in ["refs/heads/p", "refs/heads/q"].iter().enumerate() {
        let r = c
            .leader(old_leader)
            .client_write(op_create(name, 0x20 + i as u8, (i + 1) as u64))
            .await
            .unwrap();
        assert!(matches!(r.data, LedgeResp::RefUpdated(_)));
    }

    // Kill the leader: shut it down AND deregister so peers see Unreachable.
    c.node(old_leader).raft.shutdown().await.expect("shutdown");
    c.registry.deregister(c.shard, old_leader);

    // A new leader must emerge among the survivors.
    let new_leader = loop {
        let l = c.wait_for_leader().await;
        if l != old_leader {
            break l;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    };
    assert_ne!(new_leader, old_leader);

    // Previously-committed refs survive on the survivors.
    for id in [1, 2, 3] {
        if id == old_leader {
            continue;
        }
        for (i, name) in ["refs/heads/p", "refs/heads/q"].iter().enumerate() {
            let want = ObjectId::from_bytes([0x20 + i as u8; 32]);
            let mut ok = false;
            for _ in 0..200 {
                if c.node(id).sm.applied_ref(name).await.map(|e| e.target) == Some(want) {
                    ok = true;
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            assert!(ok, "survivor {id} lost committed {name}");
        }
    }

    // A new write succeeds on the new leader.
    let r = c
        .leader(new_leader)
        .client_write(op_create("refs/heads/postfail", 0x99, 100))
        .await
        .expect("post-failover write");
    assert!(matches!(r.data, LedgeResp::RefUpdated(_)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_install_to_lagging_node() {
    // Small snapshot threshold so a snapshot triggers after a handful of writes.
    let mut c = TestCluster::new_single_shard(&[1, 2, 3], Some(8)).await;
    c.initialize(1).await;
    let leader_id = c.wait_for_leader().await;
    let leader = c.leader(leader_id).clone();

    // Drive > threshold writes to force a snapshot + log purge.
    for i in 0..40u64 {
        leader
            .client_write(op_create(
                &format!("refs/heads/r{i}"),
                (i % 250) as u8 + 1,
                i + 1,
            ))
            .await
            .unwrap();
    }
    // Wait until the leader has actually built a snapshot.
    {
        let mut built = false;
        for _ in 0..200 {
            let m = leader.metrics().borrow().clone();
            if m.snapshot.is_some() {
                built = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(built, "leader never built a snapshot");
    }

    // Bring up a brand-new node 4 with empty stores, register it, add as learner.
    let n4 = c.spawn_node(4).await;
    leader
        .add_learner(4, c.members[&1].clone(), true)
        .await
        .expect("add_learner");

    // The learner must converge to the full ref set via snapshot install.
    // r39 was written with target byte (39 % 250) + 1 = 40 in the loop above.
    let probe = "refs/heads/r39";
    let want = ObjectId::from_bytes([40u8; 32]);
    let mut ok = false;
    for _ in 0..300 {
        if c.node(4).sm.applied_ref(probe).await.map(|e| e.target) == Some(want) {
            ok = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(ok, "lagging node 4 did not converge via snapshot install");
    let _ = n4;
}
