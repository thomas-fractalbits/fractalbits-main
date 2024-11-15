use cmd_lib::*;
use rayon::prelude::*;
use std::sync::Arc;
use std::sync::Mutex;
use uuid::Uuid;

use crate::ToolKind;

// Support GenUuids only for now
pub fn run_cmd_tool(ToolKind::GenUuids { num, file }: ToolKind) -> CmdResult {
    info!("Generating {num} uuids into file {file}");
    let num_threads = num_cpus::get();
    let num_uuids = (num as usize) / num_threads;

    let uuids = Arc::new(Mutex::new(Vec::new()));
    (0..num_threads).into_par_iter().for_each(|_| {
        let mut uuids_str = String::new();
        for _ in 0..num_uuids {
            uuids_str += &Uuid::now_v7().to_string();
            uuids_str += "\n";
        }
        uuids.lock().unwrap().push(uuids_str);
    });

    for uuid in uuids.lock().unwrap().iter() {
        std::fs::write(&file, uuid)?;
    }
    info!("File {file} is ready");
    Ok(())
}
