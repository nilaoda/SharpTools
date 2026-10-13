using System;
using System.IO;
using System.Diagnostics;
using System.Text;
using System.Buffers.Binary;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using Arm = System.Runtime.Intrinsics.Arm;

class Program
{
    static int Main(string[] args)
    {
        Console.OutputEncoding = Encoding.UTF8;

        if (args.Length == 0)
        {
            ProcessAllCrc64InCurrentDir();
            return 0;
        }

        if (args.Length != 1)
        {
            Console.WriteLine("Usage:");
            Console.WriteLine("  crc64ecma file.mp4");
            Console.WriteLine("  crc64ecma file.mp4.crc64");
            return 1;
        }

        string input = args[0];

        if (input.EndsWith(".crc64", StringComparison.OrdinalIgnoreCase))
            VerifyCrcList(input);
        else
            ComputeSingle(input);

        return 0;
    }

    // ================= 无参数：处理当前目录所有 crc64 =================

    static void ProcessAllCrc64InCurrentDir()
    {
        var files = Directory
            .GetFiles(Directory.GetCurrentDirectory(), "*.crc64")
            .OrderBy(f => f, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        if (files.Length == 0)
        {
            WriteColored("No .crc64 files found in current directory.", ConsoleColor.DarkGray);
            Console.WriteLine("Press any key to exit...");
            Console.ReadKey();
            return;
        }

        foreach (var file in files)
        {
            Console.WriteLine();
            WriteColored($"Processing {NameOnly(file)}", ConsoleColor.Cyan);
            VerifyCrcList(file);
        }
        Console.WriteLine("Press any key to exit...");
        Console.ReadKey();
    }

    // ================= 单文件计算 =================

    static void ComputeSingle(string file)
    {
        if (!File.Exists(file))
        {
            WriteColored($"{NameOnly(file)}  FILE NOT FOUND", ConsoleColor.DarkGray);
            return;
        }

        var timer = Stopwatch.StartNew();
        using var fs = File.OpenRead(file);
        ulong crc = Crc64Ecma.Compute(fs, p => DrawProgress(p, file));
        timer.Stop();

        Console.WriteLine();
        WriteColored($"{crc} *{NameOnly(file)}", ConsoleColor.Green);
        WriteElapsed(timer.Elapsed);
    }

    // ================= crc64 文件校验 =================

    static void VerifyCrcList(string crcFile)
    {
        if (!File.Exists(crcFile))
        {
            WriteColored($"{NameOnly(crcFile)}  FILE NOT FOUND", ConsoleColor.DarkGray);
            return;
        }

        var totalTimer = Stopwatch.StartNew();
        var lines = File.ReadAllLines(crcFile);
        int total = lines.Length;
        int index = 0;

        foreach (var line in lines)
        {
            index++;

            if (string.IsNullOrWhiteSpace(line))
                continue;

            string file;
            ulong expected;

            var legacyParts = line.Split("====", StringSplitOptions.RemoveEmptyEntries);
            if (legacyParts.Length == 2)
            {
                file = legacyParts[0].Trim();
                expected = ulong.Parse(legacyParts[1].Trim());
            }
            else
            {
                int spaceIdx = line.IndexOf(' ');
                if (spaceIdx <= 0) continue;

                string num = line.Substring(0, spaceIdx).Trim();
                string name = line.Substring(spaceIdx + 1).Trim();
                if (name.StartsWith("*")) name = name.Substring(1).Trim();

                expected = ulong.Parse(num);
                file = name;
            }

            if (!File.Exists(file))
            {
                DrawProgress(index * 100 / total, file);
                Console.WriteLine();
                WriteColored($"{NameOnly(file)}  FILE NOT FOUND", ConsoleColor.DarkGray);
                continue;
            }

            var timer = Stopwatch.StartNew();
            using var fs = File.OpenRead(file);
            ulong actual = Crc64Ecma.Compute(fs, p =>
            {
                long overall = ((index - 1) * 100 + p) / total;
                DrawProgress(overall, file);
            });
            timer.Stop();

            Console.WriteLine();

            if (actual == expected)
                WriteColored($"{NameOnly(file)}  OK  ({actual})", ConsoleColor.Green);
            else
                WriteColored(
                    $"{NameOnly(file)}  MISMATCH  ({actual} != {expected})",
                    ConsoleColor.Red
                );

            WriteElapsed(timer.Elapsed);
        }

        totalTimer.Stop();
        WriteElapsed(totalTimer.Elapsed, "Total elapsed");
    }

    // ================= 工具方法 =================

    static void WriteElapsed(TimeSpan elapsed, string label = "Elapsed")
        => WriteColored($"{label}: {elapsed.TotalSeconds:F3} s", ConsoleColor.DarkGray);

    static void DrawProgress(long percent, string path)
    {
        const int width = 28;
        int filled = (int)(percent * width / 100);

        Console.CursorLeft = 0;
        Console.Write('[');
        Console.Write(new string('█', filled));
        Console.Write(new string('-', width - filled));
        Console.Write($"] {percent,3}%  {NameOnly(path)}");
    }

    static void WriteColored(string text, ConsoleColor color)
    {
        var old = Console.ForegroundColor;
        Console.ForegroundColor = color;
        Console.WriteLine(text);
        Console.ForegroundColor = old;
    }

    static string NameOnly(string path)
        => Path.GetFileName(path);
}

// ================= CRC64-ECMA（Go 兼容，SIMD folding / slicing-by-8） =================

sealed class Crc64Ecma
{
    private const ulong Polynomial = 0xC96C5795D7870F42UL;
    private static readonly ulong[,] Table = BuildTable();
    // Reflected remainders of x^191 / x^127 and x^575 / x^511 modulo
    // x^64 + 0x42F0E1EBA9EA3693. The -1 accounts for reflected CLMUL products.
    private static readonly Vector128<ulong> Fold16 = Vector128.Create(
        0xE05DD497CA393AE4UL, 0xDABE95AFC7875F40UL);
    private static readonly Vector128<ulong> Fold64 = Vector128.Create(
        0x6AE3EFBB9DD441F3UL, 0x081F6054A7842DF4UL);
    private ulong _crc;

    private static ulong[,] BuildTable()
    {
        var table = new ulong[8, 256];

        for (ulong i = 0; i < 256; i++)
        {
            ulong crc = i;
            for (int j = 0; j < 8; j++)
                crc = (crc & 1) != 0 ? (crc >> 1) ^ Polynomial : crc >> 1;
            table[0, i] = crc;
        }

        for (int slice = 1; slice < 8; slice++)
        {
            for (int i = 0; i < 256; i++)
            {
                ulong crc = table[slice - 1, i];
                table[slice, i] = table[0, crc & 0xFF] ^ (crc >> 8);
            }
        }

        return table;
    }

    public void Update(ReadOnlySpan<byte> data)
    {
        ulong crc = ~_crc;

        // Small inputs avoid the folding setup and final reduction cost.
        if (data.Length >= 256 && BitConverter.IsLittleEndian &&
            ((Pclmulqdq.IsSupported && Sse2.IsSupported) ||
             (Arm.Aes.IsSupported && Arm.AdvSimd.Arm64.IsSupported)))
        {
            crc = UpdateSimd(crc, data, out int consumed);
            data = data.Slice(consumed);
        }

        _crc = ~UpdateScalar(crc, data);
    }

    private static ulong UpdateSimd(ulong crc, ReadOnlySpan<byte> data, out int consumed)
    {
        ref byte start = ref MemoryMarshal.GetReference(data);
        var a = LoadBlock(ref start, 0) ^ Vector128.Create(crc, 0UL);
        var b = LoadBlock(ref start, 16);
        var c = LoadBlock(ref start, 32);
        var d = LoadBlock(ref start, 48);
        int i = 64;

        // Four independent chains hide the latency of carry-less multiplication.
        while (i <= data.Length - 64)
        {
            a = Fold(a, Fold64) ^ LoadBlock(ref start, i);
            b = Fold(b, Fold64) ^ LoadBlock(ref start, i + 16);
            c = Fold(c, Fold64) ^ LoadBlock(ref start, i + 32);
            d = Fold(d, Fold64) ^ LoadBlock(ref start, i + 48);
            i += 64;
        }

        a = Fold(a, Fold16) ^ b;
        a = Fold(a, Fold16) ^ c;
        a = Fold(a, Fold16) ^ d;

        while (i <= data.Length - 16)
        {
            a = Fold(a, Fold16) ^ LoadBlock(ref start, i);
            i += 16;
        }

        consumed = i;
        // Reduce the folded 128 bits using the same reflected lookup table.
        return UpdateBlock(UpdateBlock(a.GetElement(0)) ^ a.GetElement(1));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Vector128<ulong> LoadBlock(ref byte start, int offset)
        => Vector128.LoadUnsafe(ref start, (nuint)offset).AsUInt64();

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Vector128<ulong> Fold(Vector128<ulong> value, Vector128<ulong> factors)
    {
        if (Pclmulqdq.IsSupported && Sse2.IsSupported)
        {
            return Pclmulqdq.CarrylessMultiply(value, factors, 0x00) ^
                   Pclmulqdq.CarrylessMultiply(value, factors, 0x11);
        }

        return Arm.Aes.PolynomialMultiplyWideningLower(value.GetLower(), factors.GetLower()) ^
               Arm.Aes.PolynomialMultiplyWideningUpper(value, factors);
    }

    private static ulong UpdateScalar(ulong crc, ReadOnlySpan<byte> data)
    {
        int i = 0;
        while (i <= data.Length - 8)
        {
            crc = UpdateBlock(crc ^ BinaryPrimitives.ReadUInt64LittleEndian(data.Slice(i, 8)));
            i += 8;
        }

        for (; i < data.Length; i++)
            crc = Table[0, (byte)(crc ^ data[i])] ^ (crc >> 8);

        return crc;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong UpdateBlock(ulong block)
        => Table[7, block & 0xFF] ^
           Table[6, (block >> 8) & 0xFF] ^
           Table[5, (block >> 16) & 0xFF] ^
           Table[4, (block >> 24) & 0xFF] ^
           Table[3, (block >> 32) & 0xFF] ^
           Table[2, (block >> 40) & 0xFF] ^
           Table[1, (block >> 48) & 0xFF] ^
           Table[0, block >> 56];

    public static ulong Compute(Stream stream, Action<long>? progress)
    {
        var c = new Crc64Ecma();
        byte[] buffer = new byte[1024 * 1024];

        // Length is optional for forward-only streams; seekable streams may
        // start at a nonzero position or change length while being read.
        long total = stream.CanSeek ? Math.Max(0, stream.Length - stream.Position) : 0;
        long read = 0;
        long lastPercent = -1;

        int n;
        while ((n = stream.Read(buffer, 0, buffer.Length)) > 0)
        {
            c.Update(buffer.AsSpan(0, n));
            read += n;
            if (progress != null && total > 0)
            {
                // Report completion only at EOF; decimal avoids read * 100 overflow.
                long percent = read >= total ? 99 : (long)((decimal)read * 100 / total);
                if (percent != lastPercent)
                {
                    progress(percent);
                    lastPercent = percent;
                }
            }
        }

        progress?.Invoke(100);
        return c._crc;
    }
}
