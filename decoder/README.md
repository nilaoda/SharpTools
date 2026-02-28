# decoder

基于 FFmpeg(7.0+) 的媒体信息读取工具，可输出：
- 中文树状摘要（默认）
- 纯 JSON（便于第三方解析）

在未启用 `--no-screenshot` 时，还会尝试解码并输出一张 PNG 截图。

> 使用 GPT-5.3-Codex 生成

## 命令格式

```bash
decoder <input> [--output <png>] [--no-screenshot] [--json] [--debug] [--max-probe-size <size>] [--max-analyze-seconds <sec>] [--max-read-size <size>] [--max-read-seconds <sec>]
```

`<input>` 支持本地文件路径或 FFmpeg 可识别输入源（如 `http/https/rtsp/rtp/udp/pipe/concat/...`）。

## 参数说明

- `--output <png>`
  指定截图输出路径。未指定时：
  - 本地文件输入：默认 `<input>.png`
  - 非本地文件输入源：自动提取合适文件名并保存到当前目录（如 `stream.ts.png`）
- `--no-screenshot`
  跳过解码和截图，仅做媒体信息读取。并启用更激进的快速探测预算（可通过 `--max-probe-size/--max-analyze-seconds` 覆盖）
- `--json`
  输出纯 JSON（UTF-8，中文直出）
- `--debug`
  输出实时调试步骤（带 emoji），并保留解码器原始输出；包含 HDR 判定链路（`trc/primaries/matrix/bitDepth` 原始值与来源合并结果）
- `--max-probe-size <size>`
  控制探测阶段最大读取预算（映射到 FFmpeg `probesize`），支持 `K/M/G` 后缀，例如 `8M`
- `--max-analyze-seconds <sec>`
  控制探测阶段最大分析时长（映射到 FFmpeg `analyzeduration`）
- `--max-read-size <size>`
  控制读取预算（截图解码阶段；在 `--no-screenshot` 且网络输入时用于短时包采样码率）
- `--max-read-seconds <sec>`
  控制读取最大等待时长；也会作为网络 IO 超时预算应用于网络输入

默认预算策略（未显式指定上述参数时）：
- 本地文件 + 截图：`probesize=50MiB`，`analyzeduration=10s`
- 本地文件 + `--no-screenshot`：`probesize=20MiB`，`analyzeduration=3s`
- 网络输入 + 截图：`probesize=20MiB`，`analyzeduration=5s`，`max-read-size=64MiB`，`max-read-seconds=15s`
- 网络输入 + `--no-screenshot`：`probesize=8MiB`，`analyzeduration=2s`；当容器未提供码率时再做最多 `8MiB/3s` 的短时包采样用于估算码率

## 使用示例

### 1) 默认模式（树状摘要 + 截图）

```bash
decoder "W:\Samples\AVS2.ts"
```

### 2) 指定截图路径

```bash
decoder "W:\Samples\AVS2.ts" --output "W:\out\shot.png"
```

### 3) 跳过截图（不解码）

```bash
decoder "W:\Samples\AVS2.ts" --no-screenshot
```

### 4) JSON 输出

```bash
decoder "W:\Samples\AVS2.ts" --json
```

### 5) 网络流输入 + 自动截图命名

```bash
decoder "rtsp://example.com/live/stream"
```

### 6) JSON + 调试

```bash
decoder "W:\Samples\AVS2.ts" --json --debug
```

### 7) 限制探测与读取预算（更快返回）

```bash
decoder "rtsp://example.com/live/stream" --max-probe-size 8M --max-analyze-seconds 2 --max-read-size 32M --max-read-seconds 8
```

## 输出说明

### 默认文本输出

默认为树状摘要，例如：

```text
📂 文件：W:\Samples\AVS2.ts
├─ 📊 概况
│  格式：MPEG-TS
│  大小：135.1 MiB
│  时长：0 min 31 s
│  码率：36.6 Mb/s
│
├─ 🎬 视频流 (1 路)
│  编码：AVS2
│  分辨率：3840x2160 (16:9)
│  帧率：50 FPS
│  码率：36.1 Mb/s
│
└─ 🔊 音频流 (1 路)
   └─ [1] AC-3 | 6 声道 | 448 kb/s
```

若输入不是本地文件，首行会显示 `🌐 输入源：...`。
当输入源为非本地且容器未直接给出码率时，程序会基于已读取包（时间戳 + 字节数）估算 `overall/video/audio` 码率。

若截图成功，会追加：

```text
📸 截图：W:\Samples\AVS2.ts.png
```

### JSON 输出

JSON 顶层字段：

- `input`
- `inputType`（`file` 或 `source`）
- `file`
- `decoder`（有解码时）
- `general`
- `video`（数组）
  - 每路视频包含 `hdrStatus/hdrType`、`bitDepth`、`color`（`transfer/primaries/matrix` 及对应 `*Id`）
- `audio`（数组）
- `screenshot`：`{ skipped, saved, path }`
- `debug`（启用 `--debug` 时）
- `nativeLog`（错误时可能出现）
- `error`（失败时）

## 日志行为

- 默认（不带 `--debug`）：尽量屏蔽解码器噪音日志；出错时保留原始日志
- `--debug`：实时输出调试步骤，并允许原始解码器日志直接显示
- 原生日志捕获实现已适配 Windows / Linux / macOS（不再依赖 `kernel32`）
- 说明：`--no-screenshot` 的 `20MiB` 属于探测预算（`probesize`），并非对所有协议都能严格硬限制下载字节数

## 错误输出

当参数错误或未提供输入文件时，只输出错误信息，不输出空的媒体结构。
