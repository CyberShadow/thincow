import std.algorithm.comparison;
import std.algorithm.mutation;
import std.conv;
import std.exception;
import std.file;
import std.process;
import std.random;
import std.stdio;

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

	auto pipe = .pipe();
	enum foreground = false;
	auto args = [
		"tmp/thincow",
		"--upstream=upstream",
		"--data-dir=-",
		"--block-size=" ~ blockSize.text,
		"--hash-table-size=256",
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

	foreach (_; 0 .. uniform(1, 100))
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

	enforce(spawnProcess(["fusermount", "-u", "target"]).wait() == 0, "Can't unmount");
	if (foreground) enforce(pid.wait() == 0, "thincow failed on exit");
	while (pipe.readEnd.readln()) {}

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
