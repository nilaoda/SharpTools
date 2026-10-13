### crc64ecma
复刻 Go 语言的 [crc64](https://pkg.go.dev/hash/crc64)，针对大文件进行校验。

使用`gemini-3-pro-preview`模型生成。

### 使用方法

#### 对单个大文件生成校验码
```bat
crc64ecma "影视飓风2025相机颁奖！.mp4"
```

#### 批量校验文件
准备`hash.crc64`文件，内容如下，每行一个文件：
```
影视飓风2025相机颁奖！.mp4====3838269333276814241
我的2025.mp4====17188008024935056989
```

```bat
crc64ecma "hash.crc64"
```

#### 批量校验文件+
在一个包含`.crc64`文件的文件夹下直接无参数运行`crc64ecma`，程序会扫描当前文件夹下所有`.crc64`文件，并逐个进行校验。

每个文件计算或校验完成后显示 `Elapsed: 0.123 s`，统计文件打开、读取和 CRC 计算耗时（含进度显示）。清单校验结束后另显示 `Total elapsed`，包含清单读取和全部校验耗时。

### SIMD 加速与跨平台

保持 Go `hash/crc64` 的 ECMA 表算法兼容（反射多项式 `0xC96C5795D7870F42`，初值和最终取反；`123456789` 为 `0x995DC9BBDF1939FA`）。这里的结果也称 CRC-64/XZ，与非反射的 CRC-64/ECMA-182 不同。

- x86/x64：使用 PCLMULQDQ 和 SSE2 做 128 位多项式折叠。
- ARM64：使用 PMULL 做同样的折叠，适用于 Apple Silicon 和支持该指令的 Windows/Linux ARM64 CPU。
- 运行时检测指令支持；缺少所需指令、小于 256 字节的输入及大端平台使用 slicing-by-8 查表算法。

四条独立的 SIMD 链每轮处理 64 字节，最后合并并处理余数。文件读取仍使用 1 MiB 缓冲区，流式更新和 `.crc64` 格式保持兼容，无额外 NuGet 或原生库依赖。

使用 .NET 10 SDK，建议以速度优先编译（示例为 macOS ARM64）：

```sh
dotnet publish crc64ecma/crc64ecma.cs -c Release -r osx-arm64 -p:IlcOptimizationPreference=Speed -o dist/crc64ecma
```

可将 RID 改为 `win-x64`、`win-arm64`、`osx-x64`、`linux-x64`、`linux-arm64`。NativeAOT 通常需在目标操作系统上编译，运行时自动检测 CPU 能力；不需要指定本机专用指令集。

文件处理吞吐量同时受磁盘读取和进度输出影响；SIMD 主要提升内存中的 CRC 计算速度。