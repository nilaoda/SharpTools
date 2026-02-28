# decoder

基于 FFmpeg(7.0+) 的媒体信息读取工具，可输出：
- 中文树状摘要（默认）
- 纯 JSON（便于第三方解析）

在未启用 `--no-screenshot` 时，还会尝试解码并输出一张 PNG 截图。

> 使用 GPT-5.3-Codex 生成

## 命令格式

```bash
decoder <input> [--output <png>] [--no-screenshot] [--json] [--debug]
```

## 参数说明

- `--output <png>`
  指定截图输出路径。未指定时默认：`<input>.png`
- `--no-screenshot`
  跳过解码和截图，仅做媒体信息读取
- `--json`
  输出纯 JSON（UTF-8，中文直出）
- `--debug`
  输出实时调试步骤（带 emoji），并保留解码器原始输出；包含 HDR 判定链路（`trc/primaries/matrix/bitDepth` 原始值与来源合并结果）

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

### 5) JSON + 调试

```bash
decoder "W:\Samples\AVS2.ts" --json --debug
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

若截图成功，会追加：

```text
📸 截图：W:\Samples\AVS2.ts.png
```

### JSON 输出

JSON 顶层字段：

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

## 错误输出

当参数错误或未提供输入文件时，只输出错误信息，不输出空的媒体结构。
