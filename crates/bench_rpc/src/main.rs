use anyhow::{Error, Result};
use clap::{Arg, ArgMatches, Command};
use regex::Regex;
use tokio::time::Duration;

mod bench;
mod results;
mod rpc;
mod runtime;
mod utils;

/// Matches a string like '12d 24h 5m 45s' to a regex capture.
static DURATION_MATCH: &str =
    "(?P<days>[0-9]+)d|(?P<hours>[0-9]+)h|(?P<minutes>[0-9]+)m|(?P<seconds>[0-9]+)s";

/// ReWrk
///
/// Captures CLI arguments and build benchmarking settings and runtime to
/// suite the arguments and options.
fn main() {
    rlimit::increase_nofile_limit(1000000).unwrap();

    let args = parse_args();

    let threads: usize = match args.get_one::<String>("threads").unwrap().trim().parse() {
        Ok(v) => v,
        Err(_) => {
            eprintln!("invalid parameter for 'threads' given, input type must be a integer.");
            return;
        }
    };

    let conns: usize = match args
        .get_one::<String>("connections")
        .unwrap()
        .trim()
        .parse()
    {
        Ok(v) => v,
        Err(_) => {
            eprintln!("invalid parameter for 'connections' given, input type must be a integer.");
            return;
        }
    };

    let host: &str = match args.get_one::<String>("host") {
        Some(v) => v,
        None => {
            eprintln!("missing 'host' parameter.");
            return;
        }
    };

    let json: bool = args.get_flag("json");

    let duration: &str = args
        .get_one::<String>("duration")
        .map(|s| s.as_str())
        .unwrap_or("60s");
    let duration = match parse_duration(duration) {
        Ok(dur) => dur,
        Err(e) => {
            eprintln!("failed to parse duration parameter: {e}");
            return;
        }
    };

    let keys_limit: usize = args
        .get_one::<String>("keys_limit")
        .map(|s| s.trim().parse::<usize>().unwrap_or(10_000_000))
        .unwrap_or(10_000_000);

    let pct: bool = args.get_flag("pct");

    let rounds: usize = args
        .get_one::<String>("rounds")
        .map(|s| s.trim().parse::<usize>().unwrap_or(1))
        .unwrap_or(1);

    let io_depth: usize = args
        .get_one::<String>("io_depth")
        .map(|s| s.trim().parse::<usize>().unwrap_or(1))
        .unwrap_or(1);

    let rpc = args
        .get_one::<String>("rpc")
        .map(|s| s.as_str())
        .unwrap_or("nss")
        .into();

    let input = args
        .get_one::<String>("input")
        .map(|s| s.as_str())
        .unwrap_or(if rpc == "nss" {
            "test.data"
        } else {
            "uuids.data"
        })
        .into();

    let workload = args
        .get_one::<String>("workload")
        .map(|s| s.as_str())
        .unwrap_or("write")
        .into();

    let settings = bench::BenchmarkSettings {
        threads,
        connections: conns,
        host: host.to_string(),
        duration,
        keys_limit,
        display_percentile: pct,
        display_json: json,
        rounds,
        io_depth,
        input,
        workload,
        rpc,
    };

    bench::start_benchmark(settings);
}

/// Parses a duration string from the CLI to a Duration.
/// '11d 3h 32m 4s' -> Duration
///
/// If no matches are found for the string or a invalid match
/// is captured a error message returned and displayed.
fn parse_duration(duration: &str) -> Result<Duration> {
    let mut dur = Duration::default();

    let re = Regex::new(DURATION_MATCH).unwrap();
    for cap in re.captures_iter(duration) {
        let add_to = if let Some(days) = cap.name("days") {
            let days = days.as_str().parse::<u64>()?;

            let seconds = days * 24 * 60 * 60;
            Duration::from_secs(seconds)
        } else if let Some(hours) = cap.name("hours") {
            let hours = hours.as_str().parse::<u64>()?;

            let seconds = hours * 60 * 60;
            Duration::from_secs(seconds)
        } else if let Some(minutes) = cap.name("minutes") {
            let minutes = minutes.as_str().parse::<u64>()?;

            let seconds = minutes * 60;
            Duration::from_secs(seconds)
        } else if let Some(seconds) = cap.name("seconds") {
            let seconds = seconds.as_str().parse::<u64>()?;

            Duration::from_secs(seconds)
        } else {
            return Err(Error::msg(format!("invalid match: {cap:?}")));
        };

        dur += add_to
    }

    if dur.as_secs() == 0 {
        return Err(Error::msg(format!(
            "failed to extract any valid duration from {duration}"
        )));
    }

    Ok(dur)
}

/// Contains Clap's app setup.
fn parse_args() -> ArgMatches {
    Command::new("ReWrk")
        .version("0.3.1")
        .author("Harrison Burt <hburt2003@gmail.com>")
        .about("Benchmark HTTP/1 and HTTP/2 frameworks without pipelining bias.")
        .arg(
            Arg::new("threads")
                .short('t')
                .long("threads")
                .help("Set the amount of threads to use e.g. '-t 12'")
                .value_name("NUM")
                .default_value("1"),
        )
        .arg(
            Arg::new("connections")
                .short('c')
                .long("connections")
                .help("Set the amount of concurrent e.g. '-c 512'")
                .value_name("NUM")
                .default_value("1"),
        )
        .arg(
            Arg::new("host")
                .short('h')
                .long("host")
                .help("Set the host to bench e.g. '-h http://127.0.0.1:5050'")
                .value_name("HOST")
                .required(true),
        )
        .arg(
            Arg::new("duration")
                .short('d')
                .long("duration")
                .help("Set the duration of the benchmark.")
                .value_name("DURATION"),
        )
        .arg(
            Arg::new("keys_limit")
                .short('k')
                .long("keys_limit")
                .help("Number of keys limit.")
                .value_name("NUM"),
        )
        .arg(
            Arg::new("pct")
                .long("pct")
                .help("Displays the percentile table after benchmarking.")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("json")
                .long("json")
                .help("Displays the results in a json format")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("rounds")
                .long("rounds")
                .short('r')
                .help("Repeats the benchmarks n amount of times")
                .value_name("NUM"),
        )
        .arg(
            Arg::new("io_depth")
                .long("io_depth")
                .short('D')
                .help("IO depth (number of concurrent rpc requests for one connection)")
                .value_name("NUM"),
        )
        .arg(
            Arg::new("input")
                .short('i')
                .long("input data file")
                .help("Get input data from file")
                .value_name("FILE"),
        )
        .arg(
            Arg::new("workload")
                .short('w')
                .long("workload")
                .help("Workload (read/write/mixed)")
                .value_name("TYPE"),
        )
        .arg(
            Arg::new("rpc")
                .short('p')
                .long("rpc")
                .help("rpc (nss/bss)")
                .value_name("TYPE"),
        )
        .get_matches()
}
