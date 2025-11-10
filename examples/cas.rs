use distacean::{ReadConsistency, ReadSource};
use tracing_subscriber::EnvFilter;

use crate::utils::ephemeral_distacian_cluster;
mod utils;

#[derive(serde::Deserialize, serde::Serialize, Debug, Clone, PartialEq)]
struct TestValue {
    counter: u64,
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_target(true)
        .with_thread_ids(false)
        .with_level(true)
        .with_ansi(false)
        .with_env_filter(EnvFilter::from_default_env())
        .fmt_fields(tracing_subscriber::fmt::format::DefaultFields::new())
        .init();

    let distacean = ephemeral_distacian_cluster().await?;

    // Sleep for a second to warm up
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    let kv = distacean.kv_store();

    println!("=== Testing CAS with revisions ===\n");

    // Test 1: Set initial value
    println!("1. Setting initial value");
    let set_response = kv
        .set("counter", TestValue { counter: 0 })
        .execute()
        .await
        .expect("Failed to set initial value");
    let revision1 = set_response.revision;
    println!("   Initial set returned revision: {}\n", revision1);

    // Test 2: Get with revision
    println!("2. Getting value with revision");
    let (value, revision): (TestValue, _) = kv
        .read("counter")
        .leader()
        .linearizable()
        .execute_with_revision()
        .await
        .expect("Failed to get")
        .expect("Value not found");
    println!("   Got value: {:?}, revision: {}\n", value, revision);
    assert_eq!(value.counter, 0);
    assert_eq!(revision, revision1);

    // Test 3: Successful CAS
    println!("3. Attempting CAS with correct revision");
    let cas_result = kv
        .set("counter", TestValue { counter: 1 })
        .expected_revision(revision)
        .execute()
        .await;
    match cas_result {
        Ok(response) => {
            println!("   CAS succeeded! New revision: {}\n", response.revision);
            assert_eq!(response.revision, revision + 1);
        }
        Err(e) => {
            panic!("CAS should have succeeded but failed with error: {:?}", e);
        }
    }

    // Test 4: Failed CAS with stale revision
    println!("4. Attempting CAS with stale revision");
    let cas_result = kv
        .set("counter", TestValue { counter: 999 })
        .expected_revision(revision)
        .execute()
        .await;
    match cas_result {
        Ok(response) => {
            panic!(
                "CAS should have failed but succeeded with revision: {}",
                response.revision
            );
        }
        Err(distacean::SetError::RevisionMismatch { current_revision }) => {
            println!(
                "   CAS failed as expected! Current revision: {}\n",
                current_revision
            );
            assert_eq!(current_revision, revision + 1);
        }
        Err(e) => {
            panic!("CAS failed with unexpected error: {:?}", e);
        }
    }

    // Test 5: Verify value wasn't changed
    println!("5. Verifying value after failed CAS");
    let (value, revision): (TestValue, u64) = kv
        .read("counter")
        .leader()
        .linearizable()
        .execute_with_revision()
        .await
        .expect("Failed to get")
        .expect("Value not found");
    println!("   Got value: {:?}, revision: {}\n", value, revision);
    assert_eq!(value.counter, 1);

    // Test 6: Multiple increments using CAS
    println!("6. Performing 5 increments using CAS");
    let mut current_revision = revision;
    for i in 2..=6 {
        let (current_value, _): (TestValue, u64) = kv
            .read("counter")
            .leader()
            .linearizable()
            .execute_with_revision()
            .await
            .expect("Failed to get")
            .expect("Value not found");

        match kv
            .set(
                "counter",
                TestValue {
                    counter: current_value.counter + 1,
                },
            )
            .expected_revision(current_revision)
            .execute()
            .await
        {
            Ok(response) => {
                println!(
                    "   Increment {} succeeded, revision: {}",
                    i - 1,
                    response.revision
                );
                current_revision = response.revision;
            }
            Err(_) => {
                panic!("CAS increment failed unexpectedly");
            }
        }
    }

    println!("\n7. Final value check");
    let (final_value, final_revision): (TestValue, u64) = kv
        .read("counter")
        .leader()
        .linearizable()
        .execute_with_revision()
        .await
        .expect("Failed to get")
        .expect("Value not found");
    println!(
        "   Final value: {:?}, revision: {}",
        final_value, final_revision
    );
    assert_eq!(final_value.counter, 6);

    println!("\n=== All CAS tests passed! ===");

    Ok(())
}
