import core.sys.posix.signal;
import core.sys.posix.sys.wait;
import core.sys.posix.unistd;
import core.thread.osthread;
import core.time;

import std.algorithm.comparison;
import std.algorithm.mutation;
import std.conv;
import std.exception;
import std.file;
import std.format;
import std.path;
import std.process;
import std.random;
import std.stdio;
import std.string;

import ae.sys.file : copyRecurse;

alias Seed = typeof(unpredictableSeed());

void main(string[] args)
{
	if (args.length == 1)
		while (true)
		{
			auto seed = unpredictableSeed;
			stderr.writeln("Testing seed: ", seed);
			testSeed(unpredictableSeed);
		}
	else
		foreach (seedStr; args[1..$])
			testSeed(seedStr.to!Seed);
}

void testSeed(Seed seed)
{
	rndGen.seed(seed);
	auto blockSize = 1 << uniform(3, 12);
	auto checksumBits = 1 << uniform(0, 6 + 1);
	foreach (dir; ["upstream", "target", "control"])
	{
		if (dir.exists)
			dir.rmdirRecurse();
		dir.mkdir();
	}
	auto numDevs = uniform(1, 5);
	auto block = new char[blockSize];
	foreach (d; 0 .. numDevs)
	{
		auto f = File("upstream/" ~ d.to!string, "w");
		auto len = uniform(1, 16*1024);
		foreach (b; 0 .. (len + blockSize - 1) / blockSize)
		{
			block[] = uniform!"[]"('0', '9');
			f.rawWrite(block[0 .. min($, len - b * blockSize)]);
		}
		f.close();
		copy(f.name, "control/" ~ d.to!string);
	}

	auto workDir = format("stress.%d", seed);
	mkdir(workDir);
	scope(exit) rmdirRecurse(workDir);

	auto dataDir = workDir.buildPath("data");
	mkdir(dataDir);

	auto pidFile = workDir.buildPath("thincow.pid");

	auto pipe = .pipe();
	enum foreground = false;
	auto args = [
		"tmp/thincow",
		"--upstream=upstream",
		"--data-dir=" ~ dataDir,
		"--pid-file=" ~ pidFile,
		"--block-size=" ~ blockSize.text,
		"--hash-table-size=256",
		"--checksum-bits=" ~ checksumBits.text,
		"target",
	];
	Pid pid;
	if (foreground)
	{
		pid = spawnProcess(args ~ ["--foreground", "-o=debug"], stdin, pipe.writeEnd);
		while (!exists("target/stats.txt")) {}
	}
	else
		enforce(spawnProcess(args, stdin, pipe.writeEnd).wait() == 0, "Can't start thincow");

	auto files = new File[2][numDevs];
	foreach (d; 0 .. numDevs)
		foreach (s; 0 .. 2)
			files[d][s] = File(["control/", "target/devs/"][s] ~ d.to!string, "r+");

	static char[] bufA, bufB;

	int tstpFork = fork();
	errnoEnforce(tstpFork >= 0, "fork");
	if (tstpFork == 0) // Use a fork to avoid a GC deadlock
	{
		(){
			auto pid = readText(pidFile).to!int;
			Thread.sleep(uniform(1, 100).msecs);
			kill(pid, SIGTSTP);
			while (readText(format!"/proc/%d/status"(pid)).splitLines[2][7] != 'T')
				stderr.writeln("Waiting...");
			copyRecurse(dataDir, dataDir ~ ".snapshot");
			kill(pid, SIGCONT);
			_exit(0);
		}();
	}

	foreach (_; 0 .. uniform(1, 1000))
		final switch (uniform(0, 2))
		{
			case 0: // read
			{
				auto d = uniform(0, numDevs);
				auto size = files[d][0].size;
				auto p0 = uniform(0, size);
				auto p1 = uniform(0, size);
				if (p0 > p1)
					swap(p0, p1);
				if (p0 == p1)
					continue;
				enforce(
					files[d][0].readRange(bufA, p0, p1) ==
					files[d][1].readRange(bufA, p0, p1),
					"Data mismatch");
				break;
			}
			case 1: // write
			{
				auto d = uniform(0, numDevs);
				auto size = files[d][0].size;
				auto p0 = uniform(0, size);
				auto p1 = uniform(0, size);
				if (p0 > p1)
					swap(p0, p1);
				auto len = p1 - p0;
				if (!len)
					continue;
				if (bufA.length < len)
					bufA.length = len;
				char c = 0;
				foreach (n; 0 .. len)
				{
					if (!c || (p0 + n) % blockSize == 0)
						c = uniform!"[]"('0', '9');
					bufA[n] = c;
				}
				foreach (s; 0 .. 2)
					files[d][s].writeRange(p0, bufA[0 .. len]);
				break;
			}
		}

	foreach (d; 0 .. numDevs)
		foreach (s; 0 .. 2)
			files[d][s].destroy();

	{
		int tstpStatus;
		errnoEnforce(waitpid(tstpFork, &tstpStatus, 0) == tstpFork, "waitpid");
		enforce(tstpStatus == 0, "TSTP fork failed");
	}

	enforce(spawnProcess(["fusermount", "-u", "target"]).wait() == 0, "Can't unmount");
	if (foreground) enforce(pid.wait() == 0, "thincow failed on exit");
	while (pipe.readEnd.readln()) {}

	auto fsckArgs = ["--fsck", "--no-mount", "--foreground", "-o=debug"];
	enforce(spawnProcess(args ~ fsckArgs).wait() == 0, "fsck failed");

	enforce(spawnProcess(args.replace("--data-dir=" ~ dataDir, "--data-dir=" ~ dataDir ~ ".snapshot") ~ fsckArgs).wait() == 0, "fsck of snapshot failed");

	foreach (dir; ["upstream", "target", "control"])
		dir.rmdirRecurse();
}

char[] readRange(ref File f, ref char[] buf, ulong p0, ulong p1)
{
	auto len = p1 - p0;
	if (buf.length < len)
		buf.length = len;
	f.seek(p0);
	auto res = f.rawRead(buf[0 .. len]);
	enforce(res.length == len, "Short read");
	return res;
}

void writeRange(ref File f, ulong p0, char[] data)
{
	f.seek(p0);
	f.rawWrite(data);
}
