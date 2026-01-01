# GitHub Push & EXE Build Guide

## 📦 准备推送到 GitHub

### 1. 初始化 Git（如果还没有）
```bash
git init
git add .
git commit -m "Initial commit: Quant Data Bridge with multi-exchange and proxy support"
```

### 2. 连接到 GitHub 仓库
```bash
git remote add origin https://github.com/YOUR_USERNAME/quant-data-bridge.git
git branch -M main
git push -u origin main
```

### 3. 后续更新
```bash
git add .
git commit -m "Update: describe your changes"
git push
```

---

## 🔨 打包为 EXE

### 方法 1: 使用现有的 spec 文件（推荐）

```bash
pyinstaller Quant_Data_Bridge.spec
```

### 方法 2: 从头开始打包

```bash
pyinstaller --name="Quant Data Bridge" ^
            --windowed ^
            --onefile ^
            --icon=icon.ico ^
            --add-data="ui;ui" ^
            --add-data="core;core" ^
            --add-data="utils;utils" ^
            main.py
```

### 打包后的文件位置

```
dist/
└── Quant Data Bridge.exe  # 可执行文件
```

### 测试 EXE

1. 进入 `dist` 目录
2. 双击 `Quant Data Bridge.exe`
3. 测试所有功能：
   - 马股数据获取
   - 美股数据获取
   - 期货数据获取
   - 加密货币数据获取（测试不同交易所）
   - 代理功能（如果有代理）
   - CSV 导出

---

## ✅ 发布检查清单

### 代码质量
- [ ] 所有功能正常工作
- [ ] 没有调试 print 语句（或已注释）
- [ ] 错误处理完善
- [ ] 用户提示友好

### 文档
- [ ] README.md 完整且最新
- [ ] PYINSTALLER_GUIDE.md 准确
- [ ] .gitignore 配置正确

### Git
- [ ] 临时文件已清理
- [ ] .gitignore 生效
- [ ] 提交信息清晰

### 打包
- [ ] EXE 成功生成
- [ ] EXE 可以独立运行
- [ ] 所有依赖已包含
- [ ] 文件大小合理（~50-100MB）

---

## 🚀 快速命令汇总

```bash
# 清理临时文件
Remove-Item -Recurse -Force build, dist, __pycache__ -ErrorAction SilentlyContinue

# 重新打包
pyinstaller Quant_Data_Bridge.spec

# Git 推送
git add .
git commit -m "Release v1.0: Multi-exchange support with proxy"
git push
```

---

## 📝 版本说明示例

### v1.0 Features
- ✅ 支持 4 种资产类型（马股、美股、期货、加密货币）
- ✅ 8 种时间粒度（1m ~ 1y）
- ✅ 多交易所支持（Luno, Binance, OKX, Bybit）
- ✅ 网络代理配置
- ✅ 紧凑 UI 设计
- ✅ 数据行数统计
- ✅ 智能网络错误检测
- ✅ 手动 CSV 导出
