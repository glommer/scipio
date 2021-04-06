use std::time::{Duration, Instant};

use futures_lite::StreamExt;
use tokio::runtime;
use tokio::runtime::Handle;
use tokio::sync::mpsc;

use glommio::channels::channel_mesh::{PartialMesh, Role};
use glommio::sync::Gate;
use glommio::{enclose, prelude::*};

fn main() {
    let nr_producers = 3;
    let msg_per_producer = 10_000_000;

    test_spsc_glommio_to_glommio(nr_producers, msg_per_producer);
    test_mpsc_tokio_to_tokio(nr_producers, msg_per_producer);
    test_mpsc_tokio_to_glommio(nr_producers, msg_per_producer);
}

fn test_spsc_glommio_to_glommio(nr_producers: usize, msg_per_producer: usize) {
    let nr_peers = nr_producers + 1;
    let mesh = PartialMesh::partial(nr_peers, 100);

    let t = Instant::now();

    let consumer = {
        LocalExecutorBuilder::new()
            .pin_to_cpu(0)
            .spin_before_park(Duration::from_millis(10))
            .spawn(enclose!((mesh) move || async move {
                let (_, mut receivers) = mesh.join(Role::Consumer).await.unwrap();
                let gate = Gate::new();
                for (_, stream) in receivers.streams() {
                    gate.spawn(stream.count()).unwrap().detach();
                }
                gate.close().await.unwrap()
            }))
            .unwrap()
    };

    for i in 1..nr_peers {
        LocalExecutorBuilder::new()
            .pin_to_cpu(i)
            .spin_before_park(Duration::from_millis(10))
            .spawn(enclose!((mesh) move || async move {
                let (senders, _) = mesh.join(Role::Producer).await.unwrap();
                for i in 0..msg_per_producer {
                    senders.send_to(0, i).await.unwrap();
                }
            }))
            .unwrap();
    }

    consumer.join().unwrap();
    let elapsed = t.elapsed();
    println!(
        "Glommio SPSC: Glommio -> Glommio: total time {:#?}, avg cost: {:#?}",
        elapsed,
        elapsed / msg_per_producer as _ / (nr_peers - 1) as _
    );
}

fn test_mpsc_tokio_to_tokio(nr_producers: usize, msg_per_producer: usize) {
    let nr_peers = nr_producers + 1;
    let (sender, mut receiver) = mpsc::channel(100);

    let t = Instant::now();

    let runtime = runtime::Builder::new_multi_thread()
        .worker_threads(nr_peers)
        .build()
        .unwrap();

    for _ in 0..nr_producers {
        runtime.spawn(enclose!((sender) async move {
            for i in 0..msg_per_producer {
                sender.send(i).await.unwrap();
            }
        }));
    }
    drop(sender);

    runtime.block_on(async move { while let Some(_) = receiver.recv().await {} });

    let elapsed = t.elapsed();
    println!(
        "Tokio MPSC: Tokio -> Tokio: total time {:#?}, avg cost {:#?}",
        elapsed,
        elapsed / msg_per_producer as _ / (nr_peers - 1) as _
    );
}

fn test_mpsc_tokio_to_glommio(nr_producers: usize, msg_per_producer: usize) {
    let (sender, mut receiver) = mpsc::channel(nr_producers * 100);

    let t = Instant::now();

    let consumer = {
        LocalExecutorBuilder::new()
            .pin_to_cpu(0)
            .spin_before_park(Duration::from_millis(10))
            .spawn(move || async move { while let Some(_) = receiver.recv().await {} })
            .unwrap()
    };

    let runtime = runtime::Builder::new_multi_thread()
        .worker_threads(nr_producers)
        .build()
        .unwrap();
    runtime.block_on(async move {
        for _ in 0..nr_producers {
            let sender = sender.clone();
            Handle::current().spawn(async move {
                for i in 0..msg_per_producer {
                    sender.send(i).await.unwrap();
                }
            });
        }
    });

    consumer.join().unwrap();
    let elapsed = t.elapsed();
    println!(
        "Glommio SPSC: Tokio -> Glommio: total time {:#?}, avg cost {:#?}",
        elapsed,
        elapsed / msg_per_producer as _ / nr_producers as _
    );
}
