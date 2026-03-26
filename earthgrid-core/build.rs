use std::process::Command;

fn main() {
    // Embed git commit hash at compile time
    let output = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output();
    
    let commit = match output {
        Ok(o) if o.status.success() => String::from_utf8_lossy(&o.stdout).trim().to_string(),
        _ => "unknown".to_string(),
    };
    
    println!("cargo:rustc-env=EARTHGRID_BUILD_COMMIT={}", commit);
    // Rebuild if HEAD changes
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/refs/heads/");
}
