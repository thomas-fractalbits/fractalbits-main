use std::io::Result;

fn main() -> Result<()> {
    prost_build::compile_protos(&["src/nss_ops.proto"], &["src/"])?;
    Ok(())
}
