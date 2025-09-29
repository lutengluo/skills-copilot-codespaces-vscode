# C2DB 批量下载工具

该仓库包含一个用于批量抓取 C2DB（Computational 2D Materials Database）公开网站上数据和 CIF 文件的脚本 `scripts/download_c2db.py`。脚本会遍历站点表格分页，提取材料编号，并为每个材料下载 JSON 数据集和 CIF 结构文件。

## 使用方法

安装依赖：

```bash
pip install -r requirements.txt
```

```bash
python scripts/download_c2db.py \
  --output downloads/c2db \
  --delay 1.0
```

- `--output`：保存文件的目录（默认 `downloads/c2db`）。每个材料会创建单独的子目录。
- `--delay`：两次 HTTP 请求之间的延时（秒），用于保护服务器，默认 1 秒。
- `--sid`：C2DB 前端使用的搜索编号，默认 1542（主数据集）。
- `--max-materials`：仅下载前 N 个材料，便于测试。
- `--manifest`：自定义清单文件路径。默认在输出目录生成 `manifest.json`，记录每个材料的 JSON 与 CIF 相对路径。

脚本会为每个材料保存：

- `<slug>.json`：包含 C2DB 页面提供的完整属性数据。
- `<slug>.cif`：对应的晶体结构文件。

首次运行可能需要数小时才能抓取全部材料，请合理设置 `--delay` 并避免同时多次运行。
