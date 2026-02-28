using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;

class Program
{
    private const string AVFORMAT = "avformat-61";
    private const string AVCODEC = "avcodec-61";
    private const string AVUTIL = "avutil-59";
    private const string SWSCALE = "swscale-8";

    private const int AVERROR_EOF = -541478725;
    private const int AVERROR_EAGAIN = -11;
    private const int AV_LOG_WARNING = 24;
    private const int AV_LOG_QUIET = -8;
    private const uint DUPLICATE_SAME_ACCESS = 0x00000002;
    private const int AVCOL_TRC_RESERVED0 = 0;
    private const int AVCOL_TRC_BT709 = 1;
    private const int AVCOL_TRC_UNSPECIFIED = 2;
    private const int AVCOL_TRC_RESERVED = 3;
    private const int AVCOL_TRC_GAMMA22 = 4;
    private const int AVCOL_TRC_GAMMA28 = 5;
    private const int AVCOL_TRC_SMPTE170M = 6;
    private const int AVCOL_TRC_SMPTE240M = 7;
    private const int AVCOL_TRC_BT2020_10 = 14;
    private const int AVCOL_TRC_BT2020_12 = 15;
    private const int AVCOL_TRC_SMPTE2084 = 16;
    private const int AVCOL_TRC_ARIB_STD_B67 = 18;
    private const int AVCOL_PRI_RESERVED0 = 0;
    private const int AVCOL_PRI_BT709 = 1;
    private const int AVCOL_PRI_UNSPECIFIED = 2;
    private const int AVCOL_PRI_BT2020 = 9;
    private const int AVCOL_SPC_RESERVED0 = 0;
    private const int AVCOL_SPC_BT709 = 1;
    private const int AVCOL_SPC_UNSPECIFIED = 2;
    private const int AVCOL_SPC_RESERVED = 3;
    private const int AVCOL_SPC_YCGCO = 8;
    private const int AVCOL_SPC_BT2020_NCL = 9;
    private const int AVCOL_SPC_BT2020_CL = 10;
    private const int AVCOL_SPC_ICTCP = 14;

    private static readonly HashSet<string> HardwareTags = new()
    {
        "cuvid", "qsv", "vaapi", "dxva2", "d3d11va", "videotoolbox", "mediacodec", "nvdec", "amf"
    };

    private static readonly List<string> FfmpegLogs = new List<string>();
    private static readonly List<string> DebugLogs = new List<string>();
    private static readonly av_log_callback LogCallbackDelegate = LogCallback;

    static int Main(string[] args)
    {
        Console.OutputEncoding = Encoding.UTF8;

        CliOptions cli = ParseArgs(args);
        string inputPath = cli.InputPath;
        string outputPath = string.IsNullOrEmpty(inputPath) ? "" : cli.OutputPath;
        MediaInfo info = new MediaInfo
        {
            Path = inputPath,
            Screenshot = outputPath,
            JsonOutput = cli.JsonOutput,
            SkipScreenshot = cli.SkipScreenshot,
            DebugOutput = cli.DebugOutput
        };

        IntPtr formatCtx = IntPtr.Zero;
        IntPtr codecCtx = IntPtr.Zero;
        IntPtr packet = IntPtr.Zero;
        IntPtr frame = IntPtr.Zero;
        IntPtr sws = IntPtr.Zero;
        IntPtr options = IntPtr.Zero;
        IntPtr dstBuffer = IntPtr.Zero;

        int exitCode = 0;
        CaptureContext capture = default;
        bool captureStarted = false;
        try
        {
            FfmpegLogs.Clear();
            DebugLogs.Clear();
            info.NativeLog = "";
            DebugLog(info, "⚙️ 选项: json=" + cli.JsonOutput + ", no-screenshot=" + cli.SkipScreenshot + ", debug=" + cli.DebugOutput);
            DebugLog(info, "🚀 启动解析流程");
            av_log_set_callback(LogCallbackDelegate);
            av_log_set_level(AV_LOG_QUIET);
            avformat_network_init();

            av_dict_set(ref options, "scan_all_pmts", "1", 0);
            av_dict_set(ref options, "probesize", "50000000", 0);
            av_dict_set(ref options, "analyzeduration", "10000000", 0);

            if (!string.IsNullOrEmpty(cli.Error))
            {
                DebugLog(info, "❌ 参数错误: " + cli.Error);
                info.Error = cli.Error;
                return PrintMediaInfoAndReturn(info, 1);
            }

            if (string.IsNullOrEmpty(inputPath))
            {
                DebugLog(info, "❌ 未提供输入文件");
                info.Error = "Usage: decoder <input> [--output <png>] [--no-screenshot] [--json] [--debug]";
                return PrintMediaInfoAndReturn(info, 1);
            }

            if (!File.Exists(inputPath))
            {
                DebugLog(info, "❌ 文件不存在: " + inputPath);
                info.Error = $"File not found: {inputPath}";
                return PrintMediaInfoAndReturn(info, 1);
            }
            info.General.FileSizeBytes = new FileInfo(inputPath).Length;
            DebugLog(info, "📂 输入文件: " + inputPath);
            DebugLog(info, "📏 文件大小: " + ToSize(info.General.FileSizeBytes));

            if (!cli.DebugOutput)
            {
                capture = StartNativeCapture();
                captureStarted = true;
            }

            DebugLog(info, "🔍 打开媒体输入");
            CheckError(avformat_open_input(out formatCtx, inputPath, IntPtr.Zero, ref options), "avformat_open_input");
            DebugLog(info, "🔎 分析流信息");
            CheckError(avformat_find_stream_info(formatCtx, IntPtr.Zero), "avformat_find_stream_info");
            AVFormatContext fmtForDebug = Marshal.PtrToStructure<AVFormatContext>(formatCtx);
            DebugLog(info, "📦 检测到流数量: " + fmtForDebug.nb_streams);

            int videoStreamIndex = av_find_best_stream(formatCtx, AVMediaType.Video, -1, -1, out IntPtr bestDecoder, 0);
            if (videoStreamIndex < 0)
            {
                DebugLog(info, "❌ 未找到视频流");
                throw new Exception("No video stream found.");
            }
            DebugLog(info, "🎬 选定视频流索引: " + videoStreamIndex);

            AVFrame srcFrame = default;
            bool gotFrame = false;

            if (!cli.SkipScreenshot)
            {
                AVFormatContext fmt = Marshal.PtrToStructure<AVFormatContext>(formatCtx);
                IntPtr streamPtr = Marshal.ReadIntPtr(fmt.streams, videoStreamIndex * IntPtr.Size);
                AVStream stream = Marshal.PtrToStructure<AVStream>(streamPtr);
                AVCodecParameters codecpar = Marshal.PtrToStructure<AVCodecParameters>(stream.codecpar);

                DebugLog(info, "🧩 查询可用解码器");
                var decoders = FindDecoders(codecpar.codec_id);
                if (decoders.Count == 0)
                {
                    DebugLog(info, "❌ 没有可用软件解码器");
                    throw new Exception("Cant find decoder!");
                }
                DebugLog(info, "✅ 候选解码器数: " + decoders.Count);

                var selected = decoders[^1];
                DebugLog(info, "🎯 选择解码器: " + selected.Name);
                codecCtx = avcodec_alloc_context3(selected.Codec);
                if (codecCtx == IntPtr.Zero) throw new Exception("avcodec_alloc_context3 failed.");

                CheckError(avcodec_parameters_to_context(codecCtx, stream.codecpar), "avcodec_parameters_to_context");
                av_opt_set_int(codecCtx, "skip_frame", (long)AVDiscard.NonKey, 0);
                CheckError(avcodec_open2(codecCtx, selected.Codec, IntPtr.Zero), "avcodec_open2");

                packet = av_packet_alloc();
                frame = av_frame_alloc();
                if (packet == IntPtr.Zero || frame == IntPtr.Zero) throw new Exception("alloc packet/frame failed.");

                DebugLog(info, "🎞️ 尝试解码首帧");
                gotFrame = DecodeFirstFrame(formatCtx, codecCtx, packet, frame, videoStreamIndex);
                srcFrame = gotFrame ? Marshal.PtrToStructure<AVFrame>(frame) : default;
                info.Decoder = selected.Name;
                DebugLog(info, gotFrame ? "✅ 成功解码到视频帧" : "⚠️ 未解码到可用视频帧");
            }
            else
            {
                DebugLog(info, "⏭️ 跳过解码与截图 (--no-screenshot)");
            }

            BuildMediaInfo(info, formatCtx, videoStreamIndex, srcFrame, gotFrame);
            DebugLog(info, "🧠 已构建媒体信息: 视频 " + info.Video.Count + " 路, 音频 " + info.Audio.Count + " 路");

            if (!cli.SkipScreenshot && gotFrame)
            {
                DebugLog(info, "🖼️ 开始生成截图");
                int dstPixFmt = av_get_pix_fmt("bgr24");
                if (dstPixFmt < 0) throw new Exception("av_get_pix_fmt(bgr24) failed.");

                IntPtr[] dstData = new IntPtr[4];
                int[] dstLinesize = new int[4];
                int dstBufSize = av_image_alloc(dstData, dstLinesize, srcFrame.width, srcFrame.height, dstPixFmt, 1);
                if (dstBufSize < 0) throw new Exception("av_image_alloc failed.");
                dstBuffer = dstData[0];

                sws = sws_getContext(srcFrame.width, srcFrame.height, srcFrame.format, srcFrame.width, srcFrame.height, dstPixFmt, 2, IntPtr.Zero, IntPtr.Zero, IntPtr.Zero);
                if (sws == IntPtr.Zero) throw new Exception("sws_getContext failed.");

                IntPtr[] srcData = srcFrame.data ?? throw new Exception("Decoded frame data is null.");
                int[] srcStride = srcFrame.linesize ?? throw new Exception("Decoded frame linesize is null.");
                sws_scale(sws, srcData, srcStride, 0, srcFrame.height, dstData, dstLinesize);

                SaveBgr24ToPng(dstData[0], dstLinesize[0], srcFrame.width, srcFrame.height, outputPath);
                info.ScreenshotSaved = true;
                DebugLog(info, "📸 截图已保存: " + outputPath);
            }
            else if (!cli.SkipScreenshot)
            {
                DebugLog(info, "⚠️ 未获得可用帧，未保存截图");
            }

            DebugLog(info, "🏁 处理完成");
            exitCode = 0;
        }
        catch (Exception ex)
        {
            DebugLog(info, "❌ 运行异常: " + ex.Message);
            info.Error = ex.Message;
            exitCode = 1;
        }
        finally
        {
            if (packet != IntPtr.Zero) av_packet_free(ref packet);
            if (frame != IntPtr.Zero) av_frame_free(ref frame);
            if (codecCtx != IntPtr.Zero) avcodec_free_context(ref codecCtx);
            if (formatCtx != IntPtr.Zero) avformat_close_input(ref formatCtx);
            if (sws != IntPtr.Zero) sws_freeContext(sws);
            if (dstBuffer != IntPtr.Zero) av_freep(ref dstBuffer);
            if (options != IntPtr.Zero) av_dict_free(ref options);
            if (captureStarted) info.NativeLog = StopNativeCapture(capture);
        }
        return PrintMediaInfoAndReturn(info, exitCode);
    }

    private static CliOptions ParseArgs(string[] args)
    {
        CliOptions options = new();
        for (int i = 0; i < args.Length; i++)
        {
            string arg = args[i];
            if (arg.Equals("--json", StringComparison.OrdinalIgnoreCase))
            {
                options.JsonOutput = true;
                continue;
            }
            if (arg.Equals("--debug", StringComparison.OrdinalIgnoreCase))
            {
                options.DebugOutput = true;
                continue;
            }
            if (arg.Equals("--no-screenshot", StringComparison.OrdinalIgnoreCase) || arg.Equals("--skip-screenshot", StringComparison.OrdinalIgnoreCase))
            {
                options.SkipScreenshot = true;
                continue;
            }
            if (arg.Equals("--output", StringComparison.OrdinalIgnoreCase))
            {
                if (i + 1 >= args.Length)
                {
                    options.Error = "Missing value for --output.";
                    return options;
                }
                options.OutputPath = args[++i];
                continue;
            }
            if (arg.StartsWith("--", StringComparison.Ordinal))
            {
                options.Error = "Unknown option: " + arg;
                return options;
            }
            if (string.IsNullOrEmpty(options.InputPath))
            {
                options.InputPath = arg;
                continue;
            }
            options.Error = "Too many positional arguments.";
            return options;
        }
        if (!string.IsNullOrEmpty(options.InputPath) && File.Exists(options.InputPath))
        {
            options.InputPath = ToAbsolutePath(options.InputPath);
        }
        if (!string.IsNullOrEmpty(options.InputPath) && string.IsNullOrEmpty(options.OutputPath))
        {
            options.OutputPath = options.InputPath + ".png";
        }
        else if (!string.IsNullOrEmpty(options.OutputPath))
        {
            options.OutputPath = ToAbsolutePath(options.OutputPath);
        }
        return options;
    }

    private static string ToAbsolutePath(string path)
    {
        if (string.IsNullOrWhiteSpace(path)) return path;
        try
        {
            return Path.GetFullPath(path);
        }
        catch
        {
            return path;
        }
    }

    private static void DebugLog(MediaInfo info, string message)
    {
        if (!info.DebugOutput) return;
        DebugLogs.Add(message);
        Console.Error.WriteLine("🛠️ " + message);
        Console.Error.Flush();
    }

    private static List<DecoderInfo> FindDecoders(int codecId)
    {
        List<DecoderInfo> result = new();
        IntPtr opaque = IntPtr.Zero;
        while (true)
        {
            IntPtr codecPtr = av_codec_iterate(ref opaque);
            if (codecPtr == IntPtr.Zero) break;

            if (av_codec_is_decoder(codecPtr) == 0) continue;

            AVCodec codec = Marshal.PtrToStructure<AVCodec>(codecPtr);
            if (codec.id != codecId || codec.type != (int)AVMediaType.Video) continue;

            string name = PtrToString(codec.name);
            if (string.IsNullOrEmpty(name)) continue;

            bool isHardware = false;
            foreach (var tag in HardwareTags)
            {
                if (name.Contains(tag, StringComparison.OrdinalIgnoreCase))
                {
                    isHardware = true;
                    break;
                }
            }
            if (isHardware) continue;

            result.Add(new DecoderInfo(codecPtr, name));
        }
        return result;
    }

    private static bool DecodeFirstFrame(IntPtr formatCtx, IntPtr codecCtx, IntPtr packet, IntPtr frame, int videoStreamIndex)
    {
        while (av_read_frame(formatCtx, packet) >= 0)
        {
            AVPacket pkt = Marshal.PtrToStructure<AVPacket>(packet);
            if (pkt.stream_index == videoStreamIndex)
            {
                int ret = avcodec_send_packet(codecCtx, packet);
                if (ret < 0)
                {
                    av_packet_unref(packet);
                    continue;
                }
                while (true)
                {
                    ret = avcodec_receive_frame(codecCtx, frame);
                    if (ret == AVERROR_EAGAIN || ret == AVERROR_EOF) break;
                    if (ret < 0) break;
                    av_packet_unref(packet);
                    return true;
                }
            }
            av_packet_unref(packet);
        }

        if (avcodec_send_packet(codecCtx, IntPtr.Zero) < 0) return false;
        while (true)
        {
            int flushRet = avcodec_receive_frame(codecCtx, frame);
            if (flushRet == 0) return true;
            if (flushRet == AVERROR_EAGAIN || flushRet == AVERROR_EOF) return false;
            if (flushRet < 0) return false;
        }
    }

    private static void BuildMediaInfo(MediaInfo info, IntPtr formatCtxPtr, int decodedVideoStreamIndex, AVFrame decodedFrame, bool hasDecodedFrame)
    {
        AVFormatContext fmt = Marshal.PtrToStructure<AVFormatContext>(formatCtxPtr);
        long fileSizeBytes = info.General.FileSizeBytes;
        string formatName = "";
        if (fmt.iformat != IntPtr.Zero)
        {
            AVInputFormat inputFmt = Marshal.PtrToStructure<AVInputFormat>(fmt.iformat);
            formatName = PtrToString(inputFmt.name);
        }

        info.General = new GeneralInfo
        {
            Format = formatName,
            DurationSeconds = fmt.duration > 0 ? fmt.duration / 1000000.0 : 0,
            OverallBitrate = fmt.bit_rate > 0 ? fmt.bit_rate : 0,
            FileSizeBytes = fileSizeBytes
        };
        info.Video.Clear();
        info.Audio.Clear();

        int streamCount = (int)fmt.nb_streams;
        for (int i = 0; i < streamCount; i++)
        {
            IntPtr sp = Marshal.ReadIntPtr(fmt.streams, i * IntPtr.Size);
            AVStream s = Marshal.PtrToStructure<AVStream>(sp);
            AVCodecParameters cp = Marshal.PtrToStructure<AVCodecParameters>(s.codecpar);
            double streamDuration = StreamDurationSeconds(s);
            int streamId = s.id > 0 ? s.id : s.index + 1;
            int menuId = 1;

            if (cp.codec_type == (int)AVMediaType.Video)
            {
                AVRational fps = av_guess_frame_rate(formatCtxPtr, sp, IntPtr.Zero);
                double fpsVal = fps.den == 0 ? 0 : Math.Round(Q2d(fps), 3);
                int width = cp.width;
                int height = cp.height;
                AVRational sampleAspectRatio = cp.sample_aspect_ratio;
                if (hasDecodedFrame && s.index == decodedVideoStreamIndex)
                {
                    width = decodedFrame.width;
                    height = decodedFrame.height;
                    sampleAspectRatio = decodedFrame.sample_aspect_ratio;
                }
                string codecName = PtrToString(avcodec_get_name(cp.codec_id));
                if (streamDuration <= 0) streamDuration = info.General.DurationSeconds;
                IntPtr cpPixFmtNamePtr = av_get_pix_fmt_name(cp.format);
                string cpPixFmtName = PtrToString(cpPixFmtNamePtr);
                int bitDepth = ResolveBitDepth(cp.bits_per_raw_sample, cp.bits_per_coded_sample, cpPixFmtName);
                DebugLog(info, "🌈 色彩来源[视频ID " + streamId + "] codecpar: trc=" + cp.color_trc + "(" + ColorTransferName(cp.color_trc) + ")" +
                    ", primaries=" + cp.color_primaries + "(" + ColorPrimariesName(cp.color_primaries) + ")" +
                    ", matrix=" + cp.color_space + "(" + ColorSpaceName(cp.color_space) + ")" +
                    ", bitDepth=" + bitDepth + " (raw=" + cp.bits_per_raw_sample + ", coded=" + cp.bits_per_coded_sample + ", pix_fmt=" + cp.format + "/" + cpPixFmtName + ")");

                HdrDecision codecparHdr = DetectHdr(cp.color_trc, cp.color_primaries, cp.color_space, bitDepth, "codecpar");
                HdrDecision hdr = codecparHdr;
                DebugLog(info, "🌈 HDR判定[视频ID " + streamId + "] " + codecparHdr.Reason);

                int resolvedTrc = cp.color_trc;
                int resolvedPrimaries = cp.color_primaries;
                int resolvedMatrix = cp.color_space;
                int resolvedBitDepth = bitDepth;
                if (hasDecodedFrame && s.index == decodedVideoStreamIndex)
                {
                    IntPtr framePixFmtNamePtr = av_get_pix_fmt_name(decodedFrame.format);
                    string framePixFmtName = PtrToString(framePixFmtNamePtr);
                    int frameBitDepth = ResolveBitDepth(0, 0, framePixFmtName);
                    if (frameBitDepth <= 0) frameBitDepth = bitDepth;

                    DebugLog(info, "🌈 色彩来源[视频ID " + streamId + "] decoded-frame: trc=" + decodedFrame.color_trc + "(" + ColorTransferName(decodedFrame.color_trc) + ")" +
                        ", primaries=" + decodedFrame.color_primaries + "(" + ColorPrimariesName(decodedFrame.color_primaries) + ")" +
                        ", matrix=" + decodedFrame.colorspace + "(" + ColorSpaceName(decodedFrame.colorspace) + ")" +
                        ", bitDepth=" + frameBitDepth + " (pix_fmt=" + decodedFrame.format + "/" + framePixFmtName + ")");

                    HdrDecision frameHdr = DetectHdr(decodedFrame.color_trc, decodedFrame.color_primaries, decodedFrame.colorspace, frameBitDepth, "decoded-frame");
                    DebugLog(info, "🌈 HDR判定[视频ID " + streamId + "] " + frameHdr.Reason);
                    hdr = MergeHdrDecision(codecparHdr, frameHdr);
                    DebugLog(info, "🌈 HDR采用[视频ID " + streamId + "] " + hdr.Reason);

                    resolvedTrc = MergeTransfer(decodedFrame.color_trc, cp.color_trc);
                    resolvedPrimaries = MergePrimaries(decodedFrame.color_primaries, cp.color_primaries);
                    resolvedMatrix = MergeMatrix(decodedFrame.colorspace, cp.color_space);
                    resolvedBitDepth = frameBitDepth > bitDepth ? frameBitDepth : bitDepth;
                }
                DebugLog(info, "🌈 色彩采用[视频ID " + streamId + "] trc=" + resolvedTrc + "(" + ColorTransferName(resolvedTrc) + ")" +
                    ", primaries=" + resolvedPrimaries + "(" + ColorPrimariesName(resolvedPrimaries) + ")" +
                    ", matrix=" + resolvedMatrix + "(" + ColorSpaceName(resolvedMatrix) + ")" +
                    ", bitDepth=" + resolvedBitDepth);

                VideoTrack vt = new VideoTrack
                {
                    Index = streamId,
                    Codec = codecName,
                    DurationSeconds = streamDuration,
                    MenuId = menuId,
                    Width = width,
                    Height = height,
                    FrameRate = fpsVal > 0 ? fpsVal : 0,
                    Bitrate = cp.bit_rate > 0 ? cp.bit_rate : 0,
                    HdrStatus = hdr.Status,
                    HdrType = hdr.Type,
                    ColorTransfer = resolvedTrc,
                    ColorPrimaries = resolvedPrimaries,
                    ColorMatrix = resolvedMatrix,
                    BitDepth = resolvedBitDepth,
                    SampleAspectRatioNum = sampleAspectRatio.num,
                    SampleAspectRatioDen = sampleAspectRatio.den
                };
                info.Video.Add(vt);
            }
            else if (cp.codec_type == (int)AVMediaType.Audio)
            {
                string codecName = PtrToString(avcodec_get_name(cp.codec_id));
                if (streamDuration <= 0) streamDuration = info.General.DurationSeconds;
                int ch = cp.ch_layout.nb_channels;
                if (ch <= 0) ch = 2;

                AudioTrack at = new AudioTrack
                {
                    Index = streamId,
                    Codec = codecName,
                    DurationSeconds = streamDuration,
                    MenuId = menuId,
                    Channels = ch,
                    BitrateMode = cp.bit_rate > 0 ? "Constant" : "",
                    Bitrate = cp.bit_rate > 0 ? cp.bit_rate : 0,
                };
                info.Audio.Add(at);
            }
        }

        if (info.General.DurationSeconds <= 0)
        {
            double maxDuration = 0;
            foreach (var v in info.Video) if (v.DurationSeconds > maxDuration) maxDuration = v.DurationSeconds;
            foreach (var a in info.Audio) if (a.DurationSeconds > maxDuration) maxDuration = a.DurationSeconds;
            info.General.DurationSeconds = maxDuration;
        }
        if (info.General.OverallBitrate <= 0 && info.General.DurationSeconds > 0 && info.General.FileSizeBytes > 0)
        {
            info.General.OverallBitrate = (long)(info.General.FileSizeBytes * 8 / info.General.DurationSeconds);
        }
        EstimateMissingVideoBitrate(info);
        info.General.HdrStatus = AggregateHdrStatus(info.Video);
        info.General.HdrType = AggregateHdrType(info.Video, info.General.HdrStatus);
        DebugLog(info, "🌈 HDR汇总: " + HdrStatusToText(info.General.HdrStatus, info.General.HdrType));
    }

    private static void EstimateMissingVideoBitrate(MediaInfo info)
    {
        if (info.General.OverallBitrate <= 0 || info.Video.Count == 0) return;
        long knownAudio = info.Audio.Where(a => a.Bitrate > 0).Sum(a => a.Bitrate);
        long knownVideo = info.Video.Where(v => v.Bitrate > 0).Sum(v => v.Bitrate);
        var unknownVideo = info.Video.Where(v => v.Bitrate <= 0).ToList();
        if (unknownVideo.Count == 0) return;

        long remain = info.General.OverallBitrate - knownAudio - knownVideo;
        if (remain <= 0) return;
        long perVideo = remain / unknownVideo.Count;
        if (perVideo <= 0) return;

        foreach (var v in unknownVideo)
        {
            v.Bitrate = perVideo;
        }
    }

    private static HdrDecision DetectHdr(int colorTrc, int colorPrimaries, int colorSpace, int bitDepth, string source)
    {
        if (colorTrc == AVCOL_TRC_SMPTE2084) return new HdrDecision(HdrStatus.Yes, "PQ", source + ": trc=SMPTE2084(PQ) => HDR=是");
        if (colorTrc == AVCOL_TRC_ARIB_STD_B67) return new HdrDecision(HdrStatus.Yes, "HLG", source + ": trc=ARIB_STD_B67(HLG) => HDR=是");

        bool hasBt2020Primaries = colorPrimaries == AVCOL_PRI_BT2020;
        bool hasHdrMatrix = colorSpace == AVCOL_SPC_BT2020_NCL || colorSpace == AVCOL_SPC_BT2020_CL || colorSpace == AVCOL_SPC_ICTCP;
        bool hasWideGamutHint = hasBt2020Primaries || hasHdrMatrix;
        bool hasBitDepth = bitDepth > 0;
        bool is10BitOrAbove = bitDepth >= 10;
        if (hasWideGamutHint && is10BitOrAbove)
            return new HdrDecision(HdrStatus.Yes, "", source + ": BT.2020/ICtCp + bitDepth>=10 => HDR=是(类型未知)");

        if (colorTrc == AVCOL_TRC_RESERVED0 || colorTrc == AVCOL_TRC_UNSPECIFIED || colorTrc == AVCOL_TRC_RESERVED)
        {
            bool sdrLikePrimaries = colorPrimaries == AVCOL_PRI_RESERVED0 || colorPrimaries == AVCOL_PRI_UNSPECIFIED || colorPrimaries == AVCOL_PRI_BT709;
            bool sdrLikeMatrix =
                colorSpace == AVCOL_SPC_RESERVED0 ||
                colorSpace == AVCOL_SPC_UNSPECIFIED ||
                colorSpace == AVCOL_SPC_RESERVED ||
                colorSpace == AVCOL_SPC_BT709;
            if (hasBitDepth && bitDepth <= 8 && sdrLikePrimaries && sdrLikeMatrix)
                return new HdrDecision(HdrStatus.No, "", source + ": trc未指定/保留 + 8bit + 常规色域 => 视为SDR(HDR=否)");
            return new HdrDecision(HdrStatus.Unknown, "", source + ": trc=reserved/unspecified 且无充分SDR依据 => HDR=未知");
        }

        bool explicitSdrTrc =
            colorTrc == AVCOL_TRC_BT709 ||
            colorTrc == AVCOL_TRC_GAMMA22 ||
            colorTrc == AVCOL_TRC_GAMMA28 ||
            colorTrc == AVCOL_TRC_SMPTE170M ||
            colorTrc == AVCOL_TRC_SMPTE240M;
        if (explicitSdrTrc)
        {
            // 显式 SDR 也可能来自缺失/错误元数据，避免误判为“否”。
            if (!hasBitDepth)
                return new HdrDecision(HdrStatus.Unknown, "", source + ": trc=SDR但bitDepth缺失 => HDR=未知");
            if (is10BitOrAbove || hasWideGamutHint || colorSpace == AVCOL_SPC_YCGCO)
                return new HdrDecision(HdrStatus.Unknown, "", source + ": trc=SDR但存在10bit/广色域线索 => HDR=未知");
            return new HdrDecision(HdrStatus.No, "", source + ": trc=明确SDR且8bit常规色域 => HDR=否");
        }

        if (colorTrc == AVCOL_TRC_BT2020_10 || colorTrc == AVCOL_TRC_BT2020_12)
        {
            if (is10BitOrAbove) return new HdrDecision(HdrStatus.Yes, "", source + ": trc=BT.2020_xx + >=10bit => HDR=是(类型未知)");
            return new HdrDecision(HdrStatus.Unknown, "", source + ": trc=BT.2020_xx但bitDepth缺失或不足 => HDR=未知");
        }

        if (is10BitOrAbove && (hasWideGamutHint || colorSpace == AVCOL_SPC_YCGCO))
            return new HdrDecision(HdrStatus.Unknown, "", source + ": 存在10bit+宽色域/YCoCg线索但缺少HDR传输函数 => HDR=未知");

        return new HdrDecision(HdrStatus.Unknown, "", source + ": 无明确HDR/SDR信号 => HDR=未知");
    }

    private static int ResolveBitDepth(int bitsPerRawSample, int bitsPerCodedSample, string pixFmtName)
    {
        int bitDepth = 0;
        if (bitsPerRawSample > bitDepth) bitDepth = bitsPerRawSample;
        if (bitsPerCodedSample > bitDepth) bitDepth = bitsPerCodedSample;
        int guessed = GuessBitDepthFromPixFmtName(pixFmtName);
        if (guessed > bitDepth) bitDepth = guessed;
        return bitDepth;
    }

    private static int GuessBitDepthFromPixFmtName(string pixFmtName)
    {
        if (string.IsNullOrEmpty(pixFmtName)) return 0;
        string s = pixFmtName.ToLowerInvariant();
        if (s.Contains("p16")) return 16;
        if (s.Contains("p14")) return 14;
        if (s.Contains("p12")) return 12;
        if (s.Contains("p10")) return 10;
        if (s.Contains("p9")) return 9;
        if (s.Contains("p8")) return 8;
        return 0;
    }

    private static HdrDecision MergeHdrDecision(HdrDecision codecpar, HdrDecision frame)
    {
        if (codecpar.Status == HdrStatus.Yes && frame.Status == HdrStatus.Yes)
        {
            string type = !string.IsNullOrEmpty(codecpar.Type) ? codecpar.Type : frame.Type;
            return new HdrDecision(HdrStatus.Yes, type, "codecpar+decoded-frame: 二者均判定HDR");
        }
        if (codecpar.Status == HdrStatus.Yes || frame.Status == HdrStatus.Yes)
        {
            HdrDecision yes = codecpar.Status == HdrStatus.Yes ? codecpar : frame;
            return new HdrDecision(HdrStatus.Yes, yes.Type, "codecpar+decoded-frame: 任一来源判定HDR，采用HDR");
        }
        if (codecpar.Status == HdrStatus.No && frame.Status == HdrStatus.No)
            return new HdrDecision(HdrStatus.No, "", "codecpar+decoded-frame: 二者均明确SDR，判定HDR=否");
        if (codecpar.Status == HdrStatus.No && frame.Status == HdrStatus.Unknown)
            return new HdrDecision(HdrStatus.No, "", "codecpar+decoded-frame: codecpar已明确SDR，忽略decoded-frame未知");
        if (codecpar.Status == HdrStatus.Unknown && frame.Status == HdrStatus.No)
            return new HdrDecision(HdrStatus.Unknown, "", "codecpar+decoded-frame: 仅decoded-frame判为SDR，保守输出未知");
        if (codecpar.Status == HdrStatus.Unknown || frame.Status == HdrStatus.Unknown)
            return new HdrDecision(HdrStatus.Unknown, "", "codecpar+decoded-frame: 至少一侧信息不足，判定HDR=未知");
        return codecpar;
    }

    private static bool IsTrcSpecified(int value)
    {
        return value != AVCOL_TRC_RESERVED0 && value != AVCOL_TRC_UNSPECIFIED && value != AVCOL_TRC_RESERVED;
    }

    private static bool IsPrimariesSpecified(int value)
    {
        return value != AVCOL_PRI_RESERVED0 && value != AVCOL_PRI_UNSPECIFIED;
    }

    private static bool IsMatrixSpecified(int value)
    {
        return value != AVCOL_SPC_RESERVED0 && value != AVCOL_SPC_UNSPECIFIED && value != AVCOL_SPC_RESERVED;
    }

    private static int MergeTransfer(int frameValue, int codecparValue)
    {
        if (IsTrcSpecified(codecparValue)) return codecparValue;
        if (IsTrcSpecified(frameValue)) return frameValue;
        return codecparValue;
    }

    private static int MergePrimaries(int frameValue, int codecparValue)
    {
        if (IsPrimariesSpecified(codecparValue)) return codecparValue;
        if (IsPrimariesSpecified(frameValue)) return frameValue;
        return codecparValue;
    }

    private static int MergeMatrix(int frameValue, int codecparValue)
    {
        if (IsMatrixSpecified(codecparValue)) return codecparValue;
        if (IsMatrixSpecified(frameValue)) return frameValue;
        return codecparValue;
    }

    private static string ColorTransferName(int value)
    {
        string nativeName = PtrToString(av_color_transfer_name(value));
        return value switch
        {
            AVCOL_TRC_RESERVED0 => "reserved0",
            AVCOL_TRC_BT709 => "bt709",
            AVCOL_TRC_UNSPECIFIED => "unspecified",
            AVCOL_TRC_RESERVED => "reserved",
            AVCOL_TRC_GAMMA22 => "gamma22",
            AVCOL_TRC_GAMMA28 => "gamma28",
            AVCOL_TRC_SMPTE170M => "smpte170m",
            AVCOL_TRC_SMPTE240M => "smpte240m",
            AVCOL_TRC_BT2020_10 => "bt2020_10",
            AVCOL_TRC_BT2020_12 => "bt2020_12",
            AVCOL_TRC_SMPTE2084 => "smpte2084",
            AVCOL_TRC_ARIB_STD_B67 => "arib-std-b67",
            _ => !string.IsNullOrEmpty(nativeName) ? nativeName : "unknown"
        };
    }

    private static string ColorPrimariesName(int value)
    {
        string nativeName = PtrToString(av_color_primaries_name(value));
        return value switch
        {
            AVCOL_PRI_RESERVED0 => "reserved0",
            AVCOL_PRI_BT709 => "bt709",
            AVCOL_PRI_UNSPECIFIED => "unspecified",
            AVCOL_PRI_BT2020 => "bt2020",
            _ => !string.IsNullOrEmpty(nativeName) ? nativeName : "unknown"
        };
    }

    private static string ColorSpaceName(int value)
    {
        string nativeName = PtrToString(av_color_space_name(value));
        return value switch
        {
            AVCOL_SPC_RESERVED0 => "reserved0",
            AVCOL_SPC_BT709 => "bt709",
            AVCOL_SPC_UNSPECIFIED => "unspecified",
            AVCOL_SPC_RESERVED => "reserved",
            AVCOL_SPC_YCGCO => "ycgco",
            AVCOL_SPC_BT2020_NCL => "bt2020_ncl",
            AVCOL_SPC_BT2020_CL => "bt2020_cl",
            AVCOL_SPC_ICTCP => "ictcp",
            _ => !string.IsNullOrEmpty(nativeName) ? nativeName : "unknown"
        };
    }

    private static HdrStatus AggregateHdrStatus(List<VideoTrack> videos)
    {
        if (videos.Count == 0) return HdrStatus.Unknown;
        bool hasYes = false;
        bool hasUnknown = false;
        bool hasNo = false;
        foreach (var v in videos)
        {
            if (v.HdrStatus == HdrStatus.Yes) hasYes = true;
            else if (v.HdrStatus == HdrStatus.Unknown) hasUnknown = true;
            else hasNo = true;
        }
        if (hasYes) return HdrStatus.Yes;
        if (hasUnknown) return HdrStatus.Unknown;
        if (hasNo) return HdrStatus.No;
        return HdrStatus.Unknown;
    }

    private static string AggregateHdrType(List<VideoTrack> videos, HdrStatus status)
    {
        if (status != HdrStatus.Yes) return "";
        List<string> types = new();
        foreach (var v in videos)
        {
            if (v.HdrStatus == HdrStatus.Yes && !string.IsNullOrEmpty(v.HdrType) && !types.Contains(v.HdrType))
            {
                types.Add(v.HdrType);
            }
        }
        if (types.Count == 0) return "";
        if (types.Count == 1) return types[0];
        return "Mixed";
    }

    private static string HdrStatusToJson(HdrStatus status)
    {
        return status switch
        {
            HdrStatus.Yes => "yes",
            HdrStatus.No => "no",
            _ => "unknown",
        };
    }

    private static string HdrStatusToText(HdrStatus status, string type)
    {
        if (status == HdrStatus.Yes)
        {
            return string.IsNullOrEmpty(type) ? "是" : "是 (" + type + ")";
        }
        if (status == HdrStatus.No) return "否";
        return "未知";
    }

    private static double StreamDurationSeconds(AVStream stream)
    {
        if (stream.duration <= 0) return 0;
        double tb = Q2d(stream.time_base);
        return tb <= 0 ? 0 : stream.duration * tb;
    }

    private static string NormalizeContainerFormat(string format)
    {
        if (string.IsNullOrEmpty(format)) return "";
        if (format.Equals("mpegts", StringComparison.OrdinalIgnoreCase)) return "MPEG-TS";
        return format.ToUpperInvariant();
    }

    private static string BuildMediaInfoText(MediaInfo info)
    {
        bool hasMediaSummary =
            !string.IsNullOrEmpty(info.General.Format) ||
            info.General.FileSizeBytes > 0 ||
            info.General.DurationSeconds > 0 ||
            info.Video.Count > 0 ||
            info.Audio.Count > 0;

        if (!hasMediaSummary && !string.IsNullOrEmpty(info.Error))
        {
            if (!string.IsNullOrWhiteSpace(info.NativeLog))
            {
                return info.NativeLog.TrimEnd() + Environment.NewLine + "❌ 错误：" + info.Error;
            }
            return "❌ 错误：" + info.Error;
        }

        double durationSeconds = info.General.DurationSeconds;
        long overallBitrate = info.General.OverallBitrate;
        if (overallBitrate <= 0 && durationSeconds > 0 && info.General.FileSizeBytes > 0)
            overallBitrate = (long)(info.General.FileSizeBytes * 8 / durationSeconds);

        List<string> lines = new();
        lines.Add("📂 文件：" + info.Path);
        lines.Add("├─ 📊 概况");
        if (!string.IsNullOrEmpty(info.General.Format)) lines.Add("│  格式：" + NormalizeContainerFormat(info.General.Format));
        if (info.General.FileSizeBytes > 0) lines.Add("│  大小：" + ToSize(info.General.FileSizeBytes));
        if (durationSeconds > 0) lines.Add("│  时长：" + FormatDurationSeconds(durationSeconds));
        if (overallBitrate > 0) lines.Add("│  码率：" + ToBitrate(overallBitrate));
        lines.Add("│  HDR：" + HdrStatusToText(info.General.HdrStatus, info.General.HdrType));
        lines.Add("│");

        lines.Add("├─ 🎬 视频流 (" + info.Video.Count + " 路)");
        if (info.Video.Count == 1)
        {
            var v = info.Video[0];
            string dar = GetDisplayAspectRatio(v);
            lines.Add("│  编码：" + NormalizeCodecName(v.Codec));
            if (v.Width > 0 && v.Height > 0)
            {
                string res = v.Width + "x" + v.Height;
                if (!string.IsNullOrEmpty(dar)) res += " (" + dar + ")";
                lines.Add("│  分辨率：" + res);
            }
            if (v.FrameRate > 0) lines.Add("│  帧率：" + v.FrameRate.ToString("0.###") + " FPS");
            if (v.Bitrate > 0) lines.Add("│  码率：" + ToBitrate(v.Bitrate));
            lines.Add("│  HDR：" + HdrStatusToText(v.HdrStatus, v.HdrType));
        }
        else if (info.Video.Count > 1)
        {
            for (int i = 0; i < info.Video.Count; i++)
            {
                var v = info.Video[i];
                string prefix = (i == info.Video.Count - 1) ? "│  └─ " : "│  ├─ ";
                List<string> parts = new();
                parts.Add("[" + (i + 1) + "] " + NormalizeCodecName(v.Codec));
                if (v.Width > 0 && v.Height > 0) parts.Add(v.Width + "x" + v.Height);
                if (v.FrameRate > 0) parts.Add(v.FrameRate.ToString("0.###") + " FPS");
                if (v.Bitrate > 0) parts.Add(ToBitrate(v.Bitrate));
                parts.Add("HDR " + HdrStatusToText(v.HdrStatus, v.HdrType));
                lines.Add(prefix + string.Join(" | ", parts));
            }
        }
        lines.Add("│");

        lines.Add("└─ 🔊 音频流 (" + info.Audio.Count + " 路)");
        if (info.Audio.Count == 0)
        {
            lines.Add("   └─ 无");
        }
        else
        {
            for (int i = 0; i < info.Audio.Count; i++)
            {
                var a = info.Audio[i];
                string prefix = (i == info.Audio.Count - 1) ? "   └─ " : "   ├─ ";
                List<string> parts = new();
                parts.Add("[" + (i + 1) + "] " + NormalizeCodecName(a.Codec));
                if (a.Channels > 0) parts.Add(a.Channels + " 声道");
                if (a.Bitrate > 0) parts.Add((a.Bitrate / 1000.0).ToString("0.###") + " kb/s");
                lines.Add(prefix + string.Join(" | ", parts));
            }
        }

        if (info.ScreenshotSaved)
        {
            lines.Add("");
            lines.Add("📸 截图：" + info.Screenshot);
        }

        if (!string.IsNullOrEmpty(info.Error))
        {
            lines.Add("");
            if (!string.IsNullOrWhiteSpace(info.NativeLog))
            {
                lines.Add(info.NativeLog.TrimEnd());
            }
            lines.Add("❌ 错误：" + info.Error);
        }

        return string.Join(Environment.NewLine, lines);
    }

    private static int PrintMediaInfoAndReturn(MediaInfo info, int code)
    {
        if (info.JsonOutput)
        {
            WriteMediaInfoJson(info);
        }
        else
        {
            string text = BuildMediaInfoText(info);
            Console.WriteLine(text);
        }
        return code;
    }

    private static void WriteMediaInfoJson(MediaInfo info)
    {
        double durationSeconds = info.General.DurationSeconds;
        long overallBitrate = info.General.OverallBitrate;
        if (overallBitrate <= 0 && durationSeconds > 0 && info.General.FileSizeBytes > 0)
            overallBitrate = (long)(info.General.FileSizeBytes * 8 / durationSeconds);
        var firstVideo = info.Video.Find(v => v.FrameRate > 0);

        using Stream stdout = Console.OpenStandardOutput();
        using Utf8JsonWriter writer = new Utf8JsonWriter(stdout, new JsonWriterOptions { Indented = true, Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping });

        writer.WriteStartObject();
        writer.WriteString("file", info.Path);
        if (!string.IsNullOrEmpty(info.Decoder)) writer.WriteString("decoder", info.Decoder);

        writer.WriteStartObject("general");
        writer.WriteNumber("id", 1);
        writer.WriteString("idHex", "0x1");
        writer.WriteString("format", NormalizeContainerFormat(info.General.Format));
        writer.WriteNumber("fileSizeBytes", info.General.FileSizeBytes);
        if (info.General.FileSizeBytes > 0) writer.WriteString("fileSize", ToSize(info.General.FileSizeBytes));
        writer.WriteNumber("durationSeconds", durationSeconds);
        if (durationSeconds > 0) writer.WriteString("duration", FormatDurationSeconds(durationSeconds));
        if (overallBitrate > 0)
        {
            writer.WriteString("overallBitRateMode", "Variable");
            writer.WriteNumber("overallBitRate", overallBitrate);
            writer.WriteString("overallBitRateText", ToBitrate(overallBitrate));
        }
        if (firstVideo != null) writer.WriteNumber("frameRate", firstVideo.FrameRate);
        writer.WriteEndObject();

        writer.WriteStartArray("video");
        foreach (var v in info.Video)
        {
            writer.WriteStartObject();
            writer.WriteNumber("id", v.Index);
            writer.WriteString("idHex", "0x" + v.Index.ToString("X"));
            writer.WriteNumber("menuId", v.MenuId);
            writer.WriteString("menuIdHex", "0x" + v.MenuId.ToString("X"));
            writer.WriteString("format", NormalizeCodecName(v.Codec));
            double vDuration = v.DurationSeconds > 0 ? v.DurationSeconds : durationSeconds;
            writer.WriteNumber("durationSeconds", vDuration);
            if (vDuration > 0) writer.WriteString("duration", FormatDurationSeconds(vDuration));
            if (v.Bitrate > 0)
            {
                writer.WriteNumber("bitRate", v.Bitrate);
                writer.WriteString("bitRateText", ToBitrate(v.Bitrate));
            }
            writer.WriteNumber("width", v.Width);
            writer.WriteNumber("height", v.Height);
            string dar = GetDisplayAspectRatio(v);
            if (!string.IsNullOrEmpty(dar)) writer.WriteString("displayAspectRatio", dar);
            if (v.FrameRate > 0) writer.WriteNumber("frameRate", v.FrameRate);
            if (v.BitDepth > 0) writer.WriteNumber("bitDepth", v.BitDepth);
            writer.WriteStartObject("color");
            writer.WriteNumber("transferId", v.ColorTransfer);
            writer.WriteString("transfer", ColorTransferName(v.ColorTransfer));
            writer.WriteNumber("primariesId", v.ColorPrimaries);
            writer.WriteString("primaries", ColorPrimariesName(v.ColorPrimaries));
            writer.WriteNumber("matrixId", v.ColorMatrix);
            writer.WriteString("matrix", ColorSpaceName(v.ColorMatrix));
            writer.WriteEndObject();
            writer.WriteString("hdrStatus", HdrStatusToJson(v.HdrStatus));
            if (v.HdrStatus == HdrStatus.Yes && !string.IsNullOrEmpty(v.HdrType))
                writer.WriteString("hdrType", v.HdrType);
            writer.WriteEndObject();
        }
        writer.WriteEndArray();

        writer.WriteStartArray("audio");
        foreach (var a in info.Audio)
        {
            writer.WriteStartObject();
            writer.WriteNumber("id", a.Index);
            writer.WriteString("idHex", "0x" + a.Index.ToString("X"));
            writer.WriteNumber("menuId", a.MenuId);
            writer.WriteString("menuIdHex", "0x" + a.MenuId.ToString("X"));
            writer.WriteString("format", NormalizeCodecName(a.Codec));
            double aDuration = a.DurationSeconds > 0 ? a.DurationSeconds : durationSeconds;
            writer.WriteNumber("durationSeconds", aDuration);
            if (aDuration > 0) writer.WriteString("duration", FormatDurationSeconds(aDuration));
            if (!string.IsNullOrEmpty(a.BitrateMode)) writer.WriteString("bitRateMode", a.BitrateMode);
            if (a.Bitrate > 0)
            {
                writer.WriteNumber("bitRate", a.Bitrate);
                writer.WriteString("bitRateText", (a.Bitrate / 1000.0).ToString("0.###") + " kb/s");
            }
            writer.WriteNumber("channels", a.Channels);
            writer.WriteEndObject();
        }
        writer.WriteEndArray();

        writer.WriteStartObject("screenshot");
        writer.WriteBoolean("skipped", info.SkipScreenshot);
        writer.WriteBoolean("saved", info.ScreenshotSaved);
        writer.WriteString("path", info.Screenshot);
        writer.WriteEndObject();

        if (info.DebugOutput && DebugLogs.Count > 0)
        {
            writer.WriteStartArray("debug");
            for (int i = 0; i < DebugLogs.Count; i++)
            {
                writer.WriteStringValue(NormalizeDebugForJson(DebugLogs[i]));
            }
            writer.WriteEndArray();
        }

        if (!string.IsNullOrWhiteSpace(info.NativeLog) && !string.IsNullOrEmpty(info.Error))
            writer.WriteString("nativeLog", info.NativeLog);
        if (!string.IsNullOrEmpty(info.Error)) writer.WriteString("error", info.Error);

        writer.WriteEndObject();
        writer.Flush();
        stdout.WriteByte((byte)'\n');
    }

    private static string NormalizeDebugForJson(string message)
    {
        if (string.IsNullOrEmpty(message)) return "";
        int firstSpace = message.IndexOf(' ');
        if (firstSpace > 0)
        {
            string head = message.Substring(0, firstSpace);
            bool hasLetterOrDigit = false;
            for (int i = 0; i < head.Length; i++)
            {
                if (char.IsLetterOrDigit(head[i]))
                {
                    hasLetterOrDigit = true;
                    break;
                }
            }
            if (!hasLetterOrDigit && firstSpace + 1 < message.Length)
            {
                return message.Substring(firstSpace + 1);
            }
        }
        return message;
    }

    private static string ToSize(long bytes)
    {
        const double GiB = 1024 * 1024 * 1024;
        const double MiB = 1024 * 1024;
        return bytes >= GiB
            ? (bytes / GiB).ToString("F2") + " GiB"
            : (bytes / MiB).ToString("F1") + " MiB";
    }

    private static string ToBitrate(long bps)
    {
        if (bps >= 1_000_000) return (bps / 1_000_000.0).ToString("0.0") + " Mb/s";
        if (bps >= 1_000) return (bps / 1_000.0).ToString("0.0") + " kb/s";
        return bps + " b/s";
    }

    private static string FormatDurationSeconds(double seconds)
    {
        if (seconds <= 0) return "Unknown";
        TimeSpan ts = TimeSpan.FromSeconds(seconds);
        if (ts.TotalHours >= 1)
            return ts.Hours.ToString("D2") + " h " + ts.Minutes.ToString("D2") + " min " + ts.Seconds.ToString("D2") + " s";
        return ((int)ts.TotalMinutes) + " min " + ts.Seconds.ToString("D2") + " s";
    }

    private static string GetDisplayAspectRatio(VideoTrack v)
    {
        if (v.Width <= 0 || v.Height <= 0) return "";
        int num = v.Width;
        int den = v.Height;
        if (v.SampleAspectRatioNum > 0 && v.SampleAspectRatioDen > 0)
        {
            num = v.Width * v.SampleAspectRatioNum;
            den = v.Height * v.SampleAspectRatioDen;
        }
        int g = Gcd(num, den);
        return g > 0 ? (num / g) + ":" + (den / g) : "";
    }

    private static int Gcd(int a, int b)
    {
        while (b != 0)
        {
            int t = a % b;
            a = b;
            b = t;
        }
        return Math.Abs(a);
    }

    private static string NormalizeCodecName(string codec)
    {
        if (string.IsNullOrEmpty(codec)) return "";
        return codec switch
        {
            "h264" => "AVC",
            "hevc" => "HEVC",
            "ac3" => "AC-3",
            _ => codec.ToUpperInvariant()
        };
    }

    private static void SaveBgr24ToPng(IntPtr data, int lineSize, int width, int height, string path)
    {
        int rowBytes = width * 3;
        using MemoryStream raw = new MemoryStream((rowBytes + 1) * height);
        byte[] row = new byte[rowBytes];

        for (int y = 0; y < height; y++)
        {
            IntPtr srcLine = data + y * lineSize;
            Marshal.Copy(srcLine, row, 0, rowBytes);
            raw.WriteByte(0);
            for (int x = 0; x < rowBytes; x += 3)
            {
                raw.WriteByte(row[x + 2]);
                raw.WriteByte(row[x + 1]);
                raw.WriteByte(row[x]);
            }
        }

        using MemoryStream compressed = new MemoryStream();
        using (ZLibStream zlib = new ZLibStream(compressed, CompressionLevel.Fastest, true))
        {
            byte[] rawBytes = raw.ToArray();
            zlib.Write(rawBytes, 0, rawBytes.Length);
        }

        using FileStream fs = new FileStream(path, FileMode.Create, FileAccess.Write);
        fs.Write(PngSignature, 0, PngSignature.Length);

        byte[] ihdr = new byte[13];
        WriteUInt32ToBuffer(ihdr, 0, (uint)width);
        WriteUInt32ToBuffer(ihdr, 4, (uint)height);
        ihdr[8] = 8;
        ihdr[9] = 2;
        ihdr[10] = 0;
        ihdr[11] = 0;
        ihdr[12] = 0;
        WriteChunk(fs, "IHDR", ihdr);

        byte[] idat = compressed.ToArray();
        WriteChunk(fs, "IDAT", idat);

        WriteChunk(fs, "IEND", Array.Empty<byte>());
    }

    private static readonly byte[] PngSignature = { 137, 80, 78, 71, 13, 10, 26, 10 };

    private static void WriteChunk(Stream stream, string type, byte[] data)
    {
        byte[] typeBytes = Encoding.ASCII.GetBytes(type);
        WriteUInt32(stream, (uint)data.Length);
        stream.Write(typeBytes, 0, typeBytes.Length);
        if (data.Length > 0) stream.Write(data, 0, data.Length);
        uint crc = ComputeCrc(typeBytes, data);
        WriteUInt32(stream, crc);
    }

    private static void WriteUInt32(Stream stream, uint value)
    {
        spanByte[0] = (byte)((value >> 24) & 0xFF);
        spanByte[1] = (byte)((value >> 16) & 0xFF);
        spanByte[2] = (byte)((value >> 8) & 0xFF);
        spanByte[3] = (byte)(value & 0xFF);
        stream.Write(spanByte, 0, 4);
    }

    private static void WriteUInt32ToBuffer(byte[] buffer, int offset, uint value)
    {
        buffer[offset] = (byte)((value >> 24) & 0xFF);
        buffer[offset + 1] = (byte)((value >> 16) & 0xFF);
        buffer[offset + 2] = (byte)((value >> 8) & 0xFF);
        buffer[offset + 3] = (byte)(value & 0xFF);
    }

    private static uint ComputeCrc(byte[] typeBytes, byte[] data)
    {
        uint crc = 0xFFFFFFFF;
        crc = UpdateCrc(crc, typeBytes, 0, typeBytes.Length);
        crc = UpdateCrc(crc, data, 0, data.Length);
        return crc ^ 0xFFFFFFFF;
    }

    private static uint UpdateCrc(uint crc, byte[] buf, int offset, int length)
    {
        for (int i = 0; i < length; i++)
        {
            crc = CrcTable[(crc ^ buf[offset + i]) & 0xFF] ^ (crc >> 8);
        }
        return crc;
    }

    private static readonly uint[] CrcTable = CreateCrcTable();

    private static uint[] CreateCrcTable()
    {
        uint[] table = new uint[256];
        for (uint n = 0; n < 256; n++)
        {
            uint c = n;
            for (int k = 0; k < 8; k++)
            {
                c = (c & 1) == 1 ? 0xEDB88320 ^ (c >> 1) : (c >> 1);
            }
            table[n] = c;
        }
        return table;
    }

    private static readonly byte[] spanByte = new byte[4];

    private static void CheckError(int ret, string name)
    {
        if (ret < 0) throw new Exception($"{name} failed: {GetErrorMessage(ret)}");
    }

    private static string GetErrorMessage(int error)
    {
        StringBuilder sb = new StringBuilder(1024);
        av_strerror(error, sb, (ulong)sb.Capacity);
        return sb.ToString();
    }

    private static string PtrToString(IntPtr ptr)
    {
        if (ptr == IntPtr.Zero) return "";
        try
        {
            int len = 0;
            while (Marshal.ReadByte(ptr, len) != 0) len++;
            if (len == 0) return "";
            byte[] bytes = new byte[len];
            Marshal.Copy(ptr, bytes, 0, len);
            string utf8 = Encoding.UTF8.GetString(bytes);
            if (utf8.IndexOf('\uFFFD') >= 0) return Marshal.PtrToStringAnsi(ptr) ?? utf8;
            return utf8;
        }
        catch
        {
            return Marshal.PtrToStringAnsi(ptr) ?? "";
        }
    }

    private static double Q2d(AVRational r)
    {
        if (r.den == 0) return 0;
        return (double)r.num / r.den;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct AVRational
    {
        public int num;
        public int den;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct AVFormatContext
    {
        public IntPtr av_class;
        public IntPtr iformat;
        public IntPtr oformat;
        public IntPtr priv_data;
        public IntPtr pb;
        public int ctx_flags;
        public uint nb_streams;
        public IntPtr streams;
        public IntPtr url;
        public long start_time;
        public long duration;
        public long bit_rate;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct AVStream
    {
        public IntPtr av_class;
        public int index;
        public int id;
        public IntPtr codecpar;
        public IntPtr priv_data;
        public AVRational time_base;
        public long start_time;
        public long duration;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct AVChannelLayout
    {
        public int order;
        public int nb_channels;
        public ulong mask;
        public IntPtr opaque;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct AVCodecParameters
    {
        public int codec_type;
        public int codec_id;
        public uint codec_tag;
        public IntPtr extradata;
        public int extradata_size;
        public IntPtr coded_side_data;
        public int nb_coded_side_data;
        public int format;
        public long bit_rate;
        public int bits_per_coded_sample;
        public int bits_per_raw_sample;
        public int profile;
        public int level;
        public int width;
        public int height;
        public AVRational sample_aspect_ratio;
        public AVRational framerate;
        public int field_order;
        public int color_range;
        public int color_primaries;
        public int color_trc;
        public int color_space;
        public int chroma_location;
        public int video_delay;
        public AVChannelLayout ch_layout;
        public int sample_rate;
        public int block_align;
        public int frame_size;
        public int initial_padding;
        public int trailing_padding;
        public int seek_preroll;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct AVInputFormat
    {
        public IntPtr name;
        public IntPtr long_name;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct AVCodec
    {
        public IntPtr name;
        public IntPtr long_name;
        public int type;
        public int id;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct AVPacket
    {
        public IntPtr buf;
        public long pts;
        public long dts;
        public IntPtr data;
        public int size;
        public int stream_index;
        public int flags;
        public IntPtr side_data;
        public int side_data_elems;
        public int duration;
        public long pos;
        public long convergence_duration;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct AVFrame
    {
        [MarshalAs(UnmanagedType.ByValArray, SizeConst = 8)]
        public IntPtr[] data;

        [MarshalAs(UnmanagedType.ByValArray, SizeConst = 8)]
        public int[] linesize;

        public IntPtr extended_data;
        public int width;
        public int height;
        public int nb_samples;
        public int format;
        public int key_frame;
        public int pict_type;
        public AVRational sample_aspect_ratio;
        public long pts;
        public long pkt_dts;
        public AVRational time_base;
        public int quality;
        public IntPtr opaque;
        public int repeat_pict;
        public int sample_rate;
        [MarshalAs(UnmanagedType.ByValArray, SizeConst = 8)]
        public IntPtr[] buf;
        public IntPtr extended_buf;
        public int nb_extended_buf;
        public IntPtr side_data;
        public int nb_side_data;
        public int flags;
        public int color_range;
        public int color_primaries;
        public int color_trc;
        public int colorspace;
        public int chroma_location;
        public long best_effort_timestamp;
        public IntPtr metadata;
        public int decode_error_flags;
        public IntPtr hw_frames_ctx;
        public IntPtr opaque_ref;
        public ulong crop_top;
        public ulong crop_bottom;
        public ulong crop_left;
        public ulong crop_right;
        public IntPtr private_ref;
        public AVChannelLayout ch_layout;
        public long duration;
    }

    private record DecoderInfo(IntPtr Codec, string Name);

    private enum AVMediaType
    {
        Video = 0,
        Audio = 1
    }

    private enum AVDiscard
    {
        NonKey = 32
    }

    private enum HdrStatus
    {
        Unknown = 0,
        No = 1,
        Yes = 2
    }

    private struct HdrDecision
    {
        public HdrStatus Status;
        public string Type;
        public string Reason;

        public HdrDecision(HdrStatus status, string type, string reason)
        {
            Status = status;
            Type = type;
            Reason = reason;
        }
    }

    [DllImport(AVFORMAT)]
    private static extern int avformat_open_input(out IntPtr ps, [MarshalAs(UnmanagedType.LPUTF8Str)] string url, IntPtr fmt, ref IntPtr options);

    [DllImport(AVFORMAT)]
    private static extern int avformat_find_stream_info(IntPtr ic, IntPtr options);

    [DllImport(AVFORMAT)]
    private static extern int av_find_best_stream(IntPtr ic, AVMediaType type, int wanted_stream_nb, int related_stream, out IntPtr decoder_ret, int flags);

    [DllImport(AVFORMAT)]
    private static extern void avformat_close_input(ref IntPtr s);

    [DllImport(AVFORMAT)]
    private static extern AVRational av_guess_frame_rate(IntPtr ctx, IntPtr stream, IntPtr frame);

    [DllImport(AVFORMAT)]
    private static extern int avformat_network_init();

    [DllImport(AVCODEC)]
    private static extern IntPtr avcodec_alloc_context3(IntPtr codec);

    [DllImport(AVCODEC)]
    private static extern int avcodec_parameters_to_context(IntPtr codec, IntPtr par);

    [DllImport(AVCODEC)]
    private static extern int avcodec_open2(IntPtr codec, IntPtr codecObj, IntPtr options);

    [DllImport(AVCODEC)]
    private static extern void avcodec_free_context(ref IntPtr ctx);

    [DllImport(AVCODEC)]
    private static extern IntPtr av_codec_iterate(ref IntPtr opaque);

    [DllImport(AVCODEC)]
    private static extern int av_codec_is_decoder(IntPtr codec);

    [DllImport(AVCODEC)]
    private static extern IntPtr avcodec_get_name(int id);

    [DllImport(AVCODEC)]
    private static extern IntPtr av_packet_alloc();

    [DllImport(AVCODEC)]
    private static extern void av_packet_free(ref IntPtr pkt);

    [DllImport(AVFORMAT)]
    private static extern int av_read_frame(IntPtr s, IntPtr pkt);

    [DllImport(AVCODEC)]
    private static extern void av_packet_unref(IntPtr pkt);

    [DllImport(AVCODEC)]
    private static extern int avcodec_send_packet(IntPtr avctx, IntPtr avpkt);

    [DllImport(AVCODEC)]
    private static extern int avcodec_receive_frame(IntPtr avctx, IntPtr frame);

    [DllImport(AVUTIL)]
    private static extern IntPtr av_frame_alloc();

    [DllImport(AVUTIL)]
    private static extern void av_frame_free(ref IntPtr frame);

    [DllImport(AVUTIL)]
    private static extern int av_dict_set(ref IntPtr pm, [MarshalAs(UnmanagedType.LPUTF8Str)] string key, [MarshalAs(UnmanagedType.LPUTF8Str)] string value, int flags);

    [DllImport(AVUTIL)]
    private static extern void av_dict_free(ref IntPtr m);

    [DllImport(AVUTIL)]
    private static extern int av_strerror(int errnum, StringBuilder errbuf, ulong errbuf_size);

    [DllImport(AVUTIL)]
    private static extern IntPtr av_get_pix_fmt_name(int pix_fmt);

    [DllImport(AVUTIL)]
    private static extern int av_get_pix_fmt([MarshalAs(UnmanagedType.LPUTF8Str)] string name);

    [DllImport(AVUTIL)]
    private static extern IntPtr av_color_transfer_name(int trc);

    [DllImport(AVUTIL)]
    private static extern IntPtr av_color_primaries_name(int primaries);

    [DllImport(AVUTIL)]
    private static extern IntPtr av_color_space_name(int colorspace);

    [DllImport(AVUTIL)]
    private static extern void av_freep(ref IntPtr ptr);

    [DllImport(AVUTIL)]
    private static extern int av_image_alloc([In, Out] IntPtr[] pointers, [In, Out] int[] linesizes, int w, int h, int pix_fmt, int align);

    [DllImport(AVUTIL)]
    private static extern int av_opt_set_int(IntPtr obj, [MarshalAs(UnmanagedType.LPUTF8Str)] string name, long val, int search_flags);

    [DllImport(SWSCALE)]
    private static extern IntPtr sws_getContext(int srcW, int srcH, int srcFormat, int dstW, int dstH, int dstFormat, int flags, IntPtr srcFilter, IntPtr dstFilter, IntPtr param);

    [DllImport(SWSCALE)]
    private static extern int sws_scale(IntPtr c, IntPtr[] srcSlice, int[] srcStride, int srcSliceY, int srcSliceH, IntPtr[] dst, int[] dstStride);

    [DllImport(SWSCALE)]
    private static extern void sws_freeContext(IntPtr swsContext);

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    private delegate void av_log_callback(IntPtr ptr, int level, IntPtr fmt, IntPtr vl);

    [DllImport(AVUTIL, CallingConvention = CallingConvention.Cdecl)]
    private static extern void av_log_set_level(int level);

    [DllImport(AVUTIL, CallingConvention = CallingConvention.Cdecl)]
    private static extern void av_log_set_callback(av_log_callback callback);

    [DllImport(AVUTIL, CallingConvention = CallingConvention.Cdecl)]
    private static extern int av_log_format_line2(IntPtr ptr, int level, IntPtr fmt, IntPtr vl, byte[] line, int lineSize, ref int printPrefix);

    private static void LogCallback(IntPtr ptr, int level, IntPtr fmt, IntPtr vl)
    {
        if (level > AV_LOG_WARNING) return;
        byte[] buffer = new byte[2048];
        int printPrefix = 1;
        int ret = av_log_format_line2(ptr, level, fmt, vl, buffer, buffer.Length, ref printPrefix);
        if (ret <= 0) return;
        int len = 0;
        while (len < buffer.Length && buffer[len] != 0) len++;
        if (len == 0) return;
        string line = Encoding.UTF8.GetString(buffer, 0, len).Trim();
        if (line.Length > 0) FfmpegLogs.Add(line);
    }

    private struct CaptureContext
    {
        public IntPtr OriginalOut;
        public IntPtr OriginalErr;
        public IntPtr FileHandle;
        public int SavedStdOutFd;
        public int SavedStdErrFd;
        public int TempFd;
        public string TempPath;
    }

    private static CaptureContext StartNativeCapture()
    {
        string tempPath = Path.GetTempFileName();
        IntPtr hFile = CreateFileW(tempPath, 0x40000000, 0x00000003, IntPtr.Zero, 2, 0x00000080, IntPtr.Zero);
        if (hFile == new IntPtr(-1))
        {
            return default;
        }
        IntPtr outHandle = GetStdHandle(-11);
        IntPtr errHandle = GetStdHandle(-12);
        SetStdHandle(-11, hFile);
        SetStdHandle(-12, hFile);
        int savedOutFd = -1;
        int savedErrFd = -1;
        int tempFd = -1;
        try
        {
            fflush(IntPtr.Zero);
            savedOutFd = _dup(1);
            savedErrFd = _dup(2);
            if (savedOutFd >= 0 && savedErrFd >= 0)
            {
                IntPtr dupHandle;
                if (DuplicateHandle(GetCurrentProcess(), hFile, GetCurrentProcess(), out dupHandle, 0, false, DUPLICATE_SAME_ACCESS))
                {
                    tempFd = _open_osfhandle(dupHandle, 0);
                    if (tempFd >= 0)
                    {
                        _dup2(tempFd, 1);
                        _dup2(tempFd, 2);
                    }
                    else
                    {
                        CloseHandle(dupHandle);
                    }
                }
            }
        }
        catch { }

        return new CaptureContext
        {
            OriginalOut = outHandle,
            OriginalErr = errHandle,
            FileHandle = hFile,
            SavedStdOutFd = savedOutFd,
            SavedStdErrFd = savedErrFd,
            TempFd = tempFd,
            TempPath = tempPath
        };
    }

    private static string StopNativeCapture(CaptureContext ctx)
    {
        if (ctx.FileHandle == IntPtr.Zero) return "";
        try
        {
            fflush(IntPtr.Zero);
            if (ctx.SavedStdOutFd >= 0)
            {
                _dup2(ctx.SavedStdOutFd, 1);
                _close(ctx.SavedStdOutFd);
            }
            if (ctx.SavedStdErrFd >= 0)
            {
                _dup2(ctx.SavedStdErrFd, 2);
                _close(ctx.SavedStdErrFd);
            }
            if (ctx.TempFd >= 0)
            {
                _close(ctx.TempFd);
            }
        }
        catch { }

        SetStdHandle(-11, ctx.OriginalOut);
        SetStdHandle(-12, ctx.OriginalErr);
        CloseHandle(ctx.FileHandle);
        ctx.FileHandle = IntPtr.Zero;
        string content = "";
        try
        {
            content = File.ReadAllText(ctx.TempPath, Encoding.UTF8);
            if (!string.IsNullOrWhiteSpace(content))
            {
                string[] lines = content.Split(new[] { "\r\n", "\n" }, StringSplitOptions.RemoveEmptyEntries);
                foreach (var line in lines) FfmpegLogs.Add(line.Trim());
            }
            File.Delete(ctx.TempPath);
        }
        catch { }
        return content;
    }

    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    private static extern IntPtr CreateFileW(string lpFileName, uint dwDesiredAccess, uint dwShareMode, IntPtr lpSecurityAttributes, uint dwCreationDisposition, uint dwFlagsAndAttributes, IntPtr hTemplateFile);

    [DllImport("kernel32.dll")]
    private static extern bool SetStdHandle(int nStdHandle, IntPtr hHandle);

    [DllImport("kernel32.dll")]
    private static extern IntPtr GetStdHandle(int nStdHandle);

    [DllImport("kernel32.dll")]
    private static extern bool CloseHandle(IntPtr hObject);

    [DllImport("kernel32.dll")]
    private static extern IntPtr GetCurrentProcess();

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool DuplicateHandle(IntPtr hSourceProcessHandle, IntPtr hSourceHandle, IntPtr hTargetProcessHandle, out IntPtr lpTargetHandle, uint dwDesiredAccess, bool bInheritHandle, uint dwOptions);

    [DllImport("msvcrt.dll", CallingConvention = CallingConvention.Cdecl)]
    private static extern int _dup(int fd);

    [DllImport("msvcrt.dll", CallingConvention = CallingConvention.Cdecl)]
    private static extern int _dup2(int fd1, int fd2);

    [DllImport("msvcrt.dll", CallingConvention = CallingConvention.Cdecl)]
    private static extern int _close(int fd);

    [DllImport("msvcrt.dll", CallingConvention = CallingConvention.Cdecl)]
    private static extern int _open_osfhandle(IntPtr osfhandle, int flags);

    [DllImport("msvcrt.dll", CallingConvention = CallingConvention.Cdecl)]
    private static extern int fflush(IntPtr stream);

    private class MediaInfo
    {
        public string Path { get; set; } = "";
        public string Decoder { get; set; } = "";
        public string Screenshot { get; set; } = "";
        public bool ScreenshotSaved { get; set; }
        public bool JsonOutput { get; set; }
        public bool SkipScreenshot { get; set; }
        public bool DebugOutput { get; set; }
        public string NativeLog { get; set; } = "";
        public string Error { get; set; } = "";
        public GeneralInfo General { get; set; } = new GeneralInfo();
        public List<VideoTrack> Video { get; set; } = new List<VideoTrack>();
        public List<AudioTrack> Audio { get; set; } = new List<AudioTrack>();
    }

    private class CliOptions
    {
        public string InputPath { get; set; } = "";
        public string OutputPath { get; set; } = "";
        public bool SkipScreenshot { get; set; }
        public bool JsonOutput { get; set; }
        public bool DebugOutput { get; set; }
        public string Error { get; set; } = "";
    }

    private class GeneralInfo
    {
        public string Format { get; set; } = "";
        public double DurationSeconds { get; set; }
        public long OverallBitrate { get; set; }
        public long FileSizeBytes { get; set; }
        public HdrStatus HdrStatus { get; set; } = HdrStatus.Unknown;
        public string HdrType { get; set; } = "";
    }

    private class VideoTrack
    {
        public int Index { get; set; }
        public int MenuId { get; set; }
        public string Codec { get; set; } = "";
        public double DurationSeconds { get; set; }
        public int Width { get; set; }
        public int Height { get; set; }
        public double FrameRate { get; set; }
        public long Bitrate { get; set; }
        public int BitDepth { get; set; }
        public int ColorTransfer { get; set; } = AVCOL_TRC_UNSPECIFIED;
        public int ColorPrimaries { get; set; } = AVCOL_PRI_UNSPECIFIED;
        public int ColorMatrix { get; set; } = AVCOL_SPC_UNSPECIFIED;
        public HdrStatus HdrStatus { get; set; } = HdrStatus.Unknown;
        public string HdrType { get; set; } = "";
        public int SampleAspectRatioNum { get; set; }
        public int SampleAspectRatioDen { get; set; }
    }

    private class AudioTrack
    {
        public int Index { get; set; }
        public int MenuId { get; set; }
        public string Codec { get; set; } = "";
        public double DurationSeconds { get; set; }
        public string BitrateMode { get; set; } = "Constant";
        public int Channels { get; set; }
        public long Bitrate { get; set; }
    }
}
