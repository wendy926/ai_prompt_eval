
---

## 软件设计

### 1. 功能模块

#### 1.1 数据读取模块
- **功能**：从飞书多维表格中读取对话数据。
- **实现**：`src/utils/feishu_client.py` 中的 `fetch_bitable_records` 函数。
- **输入**：
  - 飞书 App Token
  - 表格 ID
  - 视图 ID
- **输出**：包含对话内容的记录列表。

#### 1.2 数据分析模块
- **功能**：使用 LLM（Gemini 或 DeepSeek）对对话数据进行分析。
- **实现**：
  - `src/models/gemini_model.py` 中的 `GeminiDialogueAnalyzer` 类。
  - `src/models/deepseek_model.py` 中的 `DeepSeekDialogueAnalyzer` 类。
- **输入**：
  - 对话数据（JSON 格式）
  - 系统提示模板
- **输出**：分析结果（JSON 格式）。

#### 1.3 数据写入模块
- **功能**：将分析结果写入飞书多维表格。
- **实现**：`src/utils/feishu_client.py` 中的 `write_records_to_bitable` 函数。
- **输入**：
  - 飞书 App Token
  - 表格 ID
  - 分析结果
- **输出**：写入操作的成功状态。

#### 1.4 并行处理模块
- **功能**：支持对数据的批量并行处理。
- **实现**：`src/main.py` 中的 `analyze_and_write_batch` 函数。
- **输入**：
  - 数据批次
  - 模型分析器实例
- **输出**：每个批次的处理状态。

---

### 2. 数据流

1. **读取数据**：
   - 从飞书多维表格中读取对话数据。
   - 数据格式为 JSON 列表，包含 `编号`、`round5` 和 `round10` 字段。

2. **分析数据**：
   - 使用 LLM 对每条记录进行分析。
   - 系统提示模板（`system_prompt.txt`）用于指导模型生成结构化输出。

3. **写入数据**：
   - 将分析结果写入飞书多维表格。
   - 同时将结果保存到本地文件 `output.txt`。

4. **并行处理**：
   - 数据分批处理，每批次并行调用分析和写入逻辑。

---

### 3. 配置文件

#### 3.1 `.env`
- **用途**：存储环境变量。
- **关键配置**：
  - `MODEL_PROVIDER`：选择模型提供商（`gemini` 或 `deepseek`）。
  - `GOOGLE_API_KEY`：Gemini 模型的 API 密钥。
  - `DEEPSEEK_API_KEY`：DeepSeek 模型的 API 密钥。
  - `FEISHU_APP_ID` 和 `FEISHU_APP_SECRET`：飞书应用的认证信息。
  - `FEISHU_READ_APP_TOKEN` 和 `FEISHU_READ_TABLE_ID`：飞书读取表格的配置信息。

#### 3.2 `requirements.txt`
- **用途**：列出项目依赖的 Python 库。
- **关键依赖**：
  - `langchain`：用于与 LLM 交互。
  - `langchain-google-genai`：支持 Gemini 模型。
  - `lark-oapi`：飞书 API 客户端。

---

### 4. 运行流程

1. **环境准备**：
   - 安装依赖：`pip install -r requirements.txt`。
   - 配置 `.env` 文件。

2. **运行程序**：
   - 执行 `src/main.py`：`python src/main.py`。

3. **结果查看**：
   - 分析结果写入飞书多维表格。
   - 本地文件 `output.txt` 保存了所有分析结果。

---

### 5. 关键代码逻辑

#### 5.1 主程序入口
- 文件：`src/main.py`
- 函数：`main()`
- **逻辑**：
  1. 加载环境变量。
  2. 初始化模型分析器。
  3. 从飞书读取数据。
  4. 并行处理数据批次。
  5. 将结果写入飞书和本地文件。

#### 5.2 模型分析器
- 文件：`src/models/gemini_model.py` 和 `src/models/deepseek_model.py`
- 类：
  - `GeminiDialogueAnalyzer`
  - `DeepSeekDialogueAnalyzer`
- **逻辑**：
  1. 使用系统提示模板和对话数据生成分析请求。
  2. 调用 LLM API 获取分析结果。
  3. 解析和验证返回的 JSON 数据。

#### 5.3 飞书客户端
- 文件：`src/utils/feishu_client.py`
- 函数：
  - `fetch_bitable_records`
  - `write_records_to_bitable`
- **逻辑**：
  1. 使用飞书 API 读取和写入多维表格数据。
  2. 支持动态获取认证 Token。

---

### 6. 系统提示模板

- 文件：`src/prompts/system_prompt.txt`
- **用途**：指导 LLM 生成结构化分析结果。
- **内容**：
  - 包含对对话内容的评估标准和输出格式要求。
  - 使用占位符 `{{TRANSACTION}}` 动态插入对话数据。

---

### 7. 并行处理

- **实现**：`concurrent.futures.ThreadPoolExecutor`
- **逻辑**：
  1. 将数据分批，每批次大小可配置。
  2. 并行调用 `analyze_and_write_batch` 函数。
  3. 统计成功和失败的批次数量。

---

### 8. 错误处理

- **模型调用错误**：
  - 捕获 API 调用异常，记录错误日志。
  - 返回包含错误信息的 JSON。

- **数据写入错误**：
  - 捕获飞书写入失败，记录详细日志。

- **配置错误**：
  - 检查环境变量和配置文件的完整性。
  - 缺失关键配置时终止程序。

---

## 未来改进方向

1. **支持更多模型**：
   - 增加对其他 LLM 的支持，如 OpenAI GPT。

2. **优化并行处理**：
   - 动态调整批次大小和并发数以提高性能。

3. **增强错误恢复**：
   - 支持失败批次的重试机制。

4. **改进日志记录**：
   - 增加日志的分级和输出到文件的功能。

---

## 贡献指南

1. Fork 本仓库。
2. 创建新分支：`git checkout -b feature-branch-name`。
3. 提交更改：`git commit -m "Add new feature"`。
4. 推送分支：`git push origin feature-branch-name`。
5. 创建 Pull Request。

---

## 联系方式

- **作者**：Wang Yajing
- **邮箱**：wendy.wang926@gmail.com