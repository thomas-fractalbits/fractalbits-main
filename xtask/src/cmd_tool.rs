use cmd_lib::*;

pub fn run_cmd_tool(kind: String) -> CmdResult {
    assert_eq!("gen_uuids", kind);
    let num = 100_000;
    let file = "uuids.data";

    run_cmd! {
        info "Generating ${num} uuids into ${file}";
        echo -n > $file;
    }?;

    for _ in 0..num {
        let uuid = uuid::Uuid::now_v7().to_string();
        run_cmd!(echo $uuid >> $file)?;
    }
    Ok(())
}
