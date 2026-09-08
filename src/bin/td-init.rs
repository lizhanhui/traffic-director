//! td-init: minimal PID 1 for traffic-director's generational shed model.
//!
//! Generic inits (tini, dumb-init) exit when their direct child exits — but
//! a shed works by the parent spawning a child and *then exiting*, so a
//! generic init would tear down the container mid-upgrade. td-init instead:
//!
//! 1. forks and execs traffic-director in its own process group,
//! 2. forwards SIGUSR2/SIGTERM/SIGINT to that process group (all
//!    generations; during a drain window both generations treat the signal
//!    consistently),
//! 3. reaps every descendant (generations orphaned by a shed reparent to
//!    PID 1),
//! 4. exits with the last exit status only when NO descendant remains.
//!
//! Note for operators: wait for one shed to complete before signalling the
//! next.

use std::ffi::CString;
use std::os::unix::ffi::OsStrExt;
use std::process::exit;
use std::sync::atomic::{AtomicI32, Ordering};

use nix::errno::Errno;
use nix::sys::signal::{SaFlags, SigAction, SigHandler, SigSet, Signal, killpg, sigaction};
use nix::sys::wait::{WaitStatus, waitpid};
use nix::unistd::{ForkResult, execv, fork, setsid};

static PENDING: AtomicI32 = AtomicI32::new(0);

extern "C" fn record(sig: i32) {
    PENDING.store(sig, Ordering::SeqCst);
}

fn main() {
    let mut args: Vec<CString> = std::env::args_os()
        .skip(1)
        .map(|a| CString::new(a.as_os_str().as_bytes()).unwrap())
        .collect();
    if args.first().is_some_and(|a| a.as_bytes() == b"--") {
        args.remove(0);
    }
    if args.is_empty() {
        eprintln!("usage: td-init [--] <program> [args...]  (program must be an absolute path)");
        exit(2);
    }

    match unsafe { fork() } {
        Ok(ForkResult::Child) => {
            // New session/process group so the init can signal all
            // generations at once (pgid == the program's pid).
            setsid().expect("setsid failed");
            let err = execv(&args[0], &args).expect_err("exec succeeded");
            eprintln!("exec {:?} failed: {err}", args[0]);
            exit(127);
        }
        Ok(ForkResult::Parent { child }) => {
            // Generations orphaned by a shed must reparent to US (not the
            // system init), otherwise we lose track of them and exit early.
            let rc = unsafe { nix::libc::prctl(nix::libc::PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) };
            assert_eq!(rc, 0, "PR_SET_CHILD_SUBREAPER failed");
            let handler = SigAction::new(
                SigHandler::Handler(record),
                SaFlags::empty(), // no SA_RESTART: waitpid must wake on signals
                SigSet::empty(),
            );
            for sig in [Signal::SIGUSR2, Signal::SIGTERM, Signal::SIGINT] {
                unsafe { sigaction(sig, &handler) }.expect("sigaction failed");
            }

            let mut last_status = 0;
            loop {
                let pending = PENDING.swap(0, Ordering::SeqCst);
                if pending != 0 {
                    let sig = Signal::try_from(pending).expect("known signal");
                    let _ = killpg(child, sig);
                }
                match waitpid(None, None) {
                    Ok(WaitStatus::Exited(pid, code)) if pid == child => {
                        last_status = code; // direct child; descendants may live on
                    }
                    Ok(_) => {} // reaped a reparented older generation
                    Err(Errno::ECHILD) => exit(last_status), // nothing left: really done
                    Err(Errno::EINTR) => continue,
                    Err(e) => {
                        eprintln!("waitpid failed: {e}");
                        exit(1);
                    }
                }
            }
        }
        Err(e) => {
            eprintln!("fork failed: {e}");
            exit(1);
        }
    }
}
