// Runs one scheduled job: `perl supervisor.pl <job.json>`. The OS scheduler
// (launchd, systemd, cron) starts it; it holds a pid lock while the run is in
// progress and records the result in the job file and run history.
export const SUPERVISOR_SCRIPT = `#!/usr/bin/perl
use strict;
use warnings;
use JSON::PP;
use File::Basename qw(dirname);
use File::Path qw(make_path);
use POSIX qw(setsid strftime WNOHANG);
use Time::HiRes qw(time);

# opencode-scheduler supervisor v2

sub iso_now {
  my @t = localtime(time());
  return strftime("%Y-%m-%dT%H:%M:%S%z", @t);
}

sub read_json {
  my ($path) = @_;
  open my $fh, "<", $path or die "Failed to read $path: $!\\n";
  local $/;
  my $raw = <$fh>;
  close $fh;
  my $json = JSON::PP->new->utf8->relaxed;
  return $json->decode($raw);
}

sub write_json_atomic {
  my ($path, $data) = @_;
  my $tmp = "$path.tmp.$$";
  my $json = JSON::PP->new->utf8->canonical;
  open my $fh, ">", $tmp or die "Failed to write $tmp: $!\\n";
  print $fh $json->encode($data);
  close $fh or die "Failed to close $tmp: $!\\n";
  rename $tmp, $path or die "Failed to rename $tmp -> $path: $!\\n";
}

sub append_jsonl {
  my ($path, $data) = @_;
  my $json = JSON::PP->new->utf8->canonical;
  open my $fh, ">>", $path or die "Failed to append $path: $!\\n";
  print $fh $json->encode($data) . "\\n";
  close $fh;
}

sub pid_alive {
  my ($pid) = @_;
  return 0 if !$pid;
  return kill 0, $pid;
}

sub random_id {
  my $n = int(rand(1_000_000_000));
  return sprintf("%09d", $n);
}

my $job_path = shift @ARGV;
if (!$job_path) { die "usage: supervisor.pl <job.json>\\n"; }

my $job = read_json($job_path);
my $scope_id = $job->{scopeId} || "";
my $slug = $job->{slug} || "";
if (!$scope_id || !$slug) { die "job missing scopeId/slug\\n"; }

# Applies run state to the job file as it is now, so edits made while the run
# is in progress (update_job) are kept. Returns 0 without writing when the job
# has been deleted, so a finishing run cannot bring it back.
sub update_job_state {
  my ($set, @clear) = @_;
  my $current;
  for my $attempt (1 .. 5) {
    return 0 if !-e $job_path;
    $current = eval { read_json($job_path) };
    last if $current && ref($current) eq 'HASH';
    select(undef, undef, undef, 0.05);
  }
  $current = $job if !$current || ref($current) ne 'HASH';
  delete $current->{$_} for @clear;
  $current->{$_} = $set->{$_} for keys %$set;
  write_json_atomic($job_path, $current);
  return 1;
}

my $home = $ENV{HOME} || "";
if (!$home) { die "HOME is not set\\n"; }

my $config_root = "$home/.config/opencode";
my $scheduler_root = "$config_root/scheduler/scopes/$scope_id";
my $locks_dir = "$scheduler_root/locks";
my $runs_dir = "$scheduler_root/runs";
my $logs_dir = "$config_root/logs/scheduler/$scope_id";

make_path($locks_dir);
make_path($runs_dir);
make_path($logs_dir);

my $log_path = "$logs_dir/$slug.log";
open STDOUT, ">>", $log_path or die "Failed to open log $log_path: $!\\n";
open STDERR, ">&STDOUT" or die "Failed to dup stderr: $!\\n";
select STDOUT; $| = 1;
select STDERR; $| = 1;

my $lock_path = "$locks_dir/$slug.json";
if (-e $lock_path) {
  my $lock = eval { read_json($lock_path) };
  my $pid = ($lock && ref($lock) eq 'HASH') ? ($lock->{pid} || 0) : 0;
  if (pid_alive($pid)) {
    my $now = iso_now();
    print "\\n=== Scheduled run skipped (already running pid=$pid) $now ===\\n";
    exit 0;
  }
  unlink $lock_path;
}

my $run_id = time() . "-" . random_id();
my $started_at = iso_now();
my $t0 = time();

write_json_atomic($lock_path, { pid => $$, startedAt => $started_at, runId => $run_id });

my $started = update_job_state(
  { lastRunAt => $started_at, lastRunSource => "scheduled", lastRunStatus => "running", updatedAt => $started_at },
  "lastRunExitCode", "lastRunError",
);
if (!$started) {
  print "\\n=== Scheduled run skipped (job deleted) $started_at ===\\n";
  unlink $lock_path;
  exit 0;
}

# Force non-interactive scheduled runs
my $perm = { question => "deny" };
if ($ENV{OPENCODE_PERMISSION}) {
  my $existing = eval { JSON::PP->new->decode($ENV{OPENCODE_PERMISSION}) };
  if ($existing && ref($existing) eq 'HASH') {
    $perm = { %$existing, %$perm };
  }
}
$ENV{OPENCODE_PERMISSION} = JSON::PP->new->canonical->encode($perm);
$ENV{OPENCODE_SCHEDULER_RUN_ID} = $run_id;

print "\\n=== Scheduled run $started_at runId=$run_id ===\\n";

my $inv = $job->{invocation};
if (!$inv || ref($inv) ne 'HASH' || !$inv->{command} || ref($inv->{args}) ne 'ARRAY') {
  my $now = iso_now();
  print "\\n=== Supervisor error $now: job missing invocation.command/args ===\\n";
  update_job_state({ lastRunStatus => "failed", lastRunError => "job missing invocation", updatedAt => $now });
  unlink $lock_path;
  exit 1;
}

my $command = $inv->{command};
my @args = @{ $inv->{args} };

my $workdir = $job->{workdir} || $home;

my $timeout = $job->{timeoutSeconds};
$timeout = undef if defined($timeout) && $timeout !~ /^\\d+$/;

my $timed_out = 0;
my $terminated = 0;
my $reaped_status;
my $child_pid = fork();
if (!defined $child_pid) {
  my $now = iso_now();
  print "\\n=== Supervisor error $now: fork failed: $! ===\\n";
  update_job_state({ lastRunStatus => "failed", lastRunError => "fork failed", updatedAt => $now });
  unlink $lock_path;
  exit 1;
}

if ($child_pid == 0) {
  chdir $workdir or die "Failed to chdir to $workdir: $!\\n";
  # OpenCode 2 \`run\` resolves its directory from PWD before cwd.
  $ENV{PWD} = $workdir;
  eval { setsid(); };
  exec { $command } $command, @args;
  die "Failed to exec $command: $!\\n";
}

# The run is in its own session, so signals sent to the supervisor do not
# reach it. Record its pid so the run can be found and stopped.
write_json_atomic($lock_path, { pid => $$, childPid => $child_pid, startedAt => $started_at, runId => $run_id });

# Sends SIGTERM to the run's process group, waits up to $grace seconds for it
# to exit, then sends SIGKILL. Runs from signal handlers, which can fire just
# after the main waitpid returned, so it must not touch that waitpid's $?.
sub stop_child {
  my ($grace) = @_;
  local $?;
  kill 'TERM', -$child_pid;
  my $deadline = time() + $grace;
  while (time() < $deadline) {
    my $reaped = waitpid($child_pid, WNOHANG);
    if ($reaped == $child_pid) {
      $reaped_status = $?;
      return;
    }
    # Already reaped by the main waitpid.
    return if $reaped == -1;
    select(undef, undef, undef, 0.1);
  }
  my $now = iso_now();
  print "\\n=== Forcing SIGKILL $now ===\\n";
  kill 'KILL', -$child_pid;
}

# delete_job, launchctl unload and systemctl stop end a run by signalling the
# supervisor; pass it on to the run instead of leaving the run orphaned.
for my $sig (qw(TERM INT HUP)) {
  $SIG{$sig} = sub {
    return if $terminated || $timed_out;
    $terminated = 1;
    my $now = iso_now();
    print "\\n=== Received SIG$sig $now; stopping run ===\\n";
    stop_child(5);
  };
}

if (defined($timeout) && $timeout > 0) {
  $SIG{ALRM} = sub {
    return if $terminated;
    $timed_out = 1;
    my $now = iso_now();
    print "\\n=== Timeout after $timeout seconds $now; sending SIGTERM ===\\n";
    stop_child(5);
  };
  alarm($timeout);
}

my $waited = waitpid($child_pid, 0);
my $status = $?;
alarm(0);
if ($waited != $child_pid && defined $reaped_status) {
  # A signal handler reaped the run while waitpid was interrupted.
  $waited = $child_pid;
  $status = $reaped_status;
}

my $finished_at = iso_now();
my $duration_ms = int((time() - $t0) * 1000);
my $signal = $status & 127;
my $exit_code = $signal ? 128 + $signal : ($status >> 8);
$exit_code = 124 if $timed_out;

my $final_status = "failed";
my $final_error = undef;
if ($timed_out) {
  $final_error = "timeout";
} elsif ($terminated) {
  $final_error = "stopped";
} elsif ($waited != $child_pid) {
  $final_error = "waitpid failed";
} elsif ($status == 0) {
  $final_status = "success";
} elsif ($signal) {
  $final_error = "killed by signal $signal";
} else {
  $final_error = "exit code $exit_code";
}

my %final = (lastRunStatus => $final_status, lastRunExitCode => $exit_code, updatedAt => $finished_at);
$final{lastRunError} = $final_error if defined $final_error;
my $recorded = update_job_state(\\%final, defined $final_error ? () : ("lastRunError"));

if ($recorded) {
  append_jsonl("$runs_dir/$slug.jsonl", {
    runId => $run_id,
    scopeId => $scope_id,
    slug => $slug,
    startedAt => $started_at,
    finishedAt => $finished_at,
    durationMs => $duration_ms,
    status => $final_status,
    exitCode => $exit_code,
    error => $final_error,
    pid => $child_pid,
    logPath => $log_path,
  });
}

unlink $lock_path;
my $note = $recorded ? "" : " (job deleted; result not recorded)";
print "\\n=== Finished $finished_at status=$final_status exitCode=$exit_code durationMs=$duration_ms$note ===\\n";
exit($exit_code);
`
