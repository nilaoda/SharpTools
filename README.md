# SharpTools
存放一些 C# 编写的单文件工具

所有文件夹都是独立的，不依赖`.csproj`项目文件，可独立运行的`.cs`文件。

你可以轻松将它们编译成二进制分发：

## 1. 使用 dotnet 原生编译 (推荐)
下载 [.NET 10 SDK](https://dotnet.microsoft.com/en-US/download/dotnet/10.0) 或更高版本

```bat
dotnet publish source.cs -o . -r win-x64 -c Release -p:TrimMode=link -p:StackTraceSupport=false -p:InvariantGlobalization=true -p:DebugType=none -p:DebugSymbols=false -p:IlcOptimizationPreference=Size
```

编译完成后会在当前目录下生成对应的二进制文件，可直接执行。

如果想为其他系统编译，只需要修改`-r`，如`osx-arm64`/`osx-x64`。

## 2. 使用 bflat 编译

https://github.com/bflattened/bflat

```bat
bflat build source.cs -o dist.exe -Os --no-reflection --no-stacktrace-data --no-exception-messages --no-pie --no-debug-info
```

## 特别注意
本仓库包含大量 AI 生成的代码，使用时需要自行判断。