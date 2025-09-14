pub mod leader_election;
pub mod multi_az;

use crate::{
    CmdResult, InitConfig, MultiAzTestType, ServiceName, TestType,
    cmd_build::{self, BuildMode},
    cmd_service,
};

pub async fn run_tests(test_type: TestType) -> CmdResult {
    let test_leader_election = || async {
        cmd_service::init_service(ServiceName::All, BuildMode::Debug, InitConfig::default())?;
        cmd_service::start_service(ServiceName::DdbLocal)?;
        leader_election::run_leader_election_tests().await?;
        leader_election::cleanup_test_root_server_instances()?;
        Ok(())
    };

    // prepare
    cmd_service::stop_service(ServiceName::All)?;
    cmd_build::build_rust_servers(BuildMode::Debug)?;
    match test_type {
        TestType::MultiAz { subcommand } => multi_az::run_multi_az_tests(subcommand).await,
        TestType::LeaderElection => test_leader_election().await,
        TestType::All => {
            test_leader_election().await?;
            multi_az::run_multi_az_tests(MultiAzTestType::All).await
        }
    }
}
