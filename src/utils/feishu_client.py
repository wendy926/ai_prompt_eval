import logging
import json
import os
from typing import List, Optional, Dict, Any

# 导入 lark-oapi 相关模块
import lark_oapi as lark
from lark_oapi import LogLevel # 导入日志级别
# 更新导入，使用 AppTableRecord 相关的类 (读取)
from lark_oapi.api.bitable.v1 import ListAppTableRecordRequest, ListAppTableRecordResponse
# 新增导入：用于获取 tenant_access_token
from lark_oapi.api.auth.v3 import InternalTenantAccessTokenRequest, InternalTenantAccessTokenRequestBody, InternalTenantAccessTokenResponse
# 新增导入：用于写入多维表格记录
from lark_oapi.api.bitable.v1 import BatchCreateAppTableRecordRequest, BatchCreateAppTableRecordRequestBody, AppTableRecord

# 配置日志
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# --- 新增：获取 Tenant Access Token 的函数 ---
# --- 修改：获取 Tenant Access Token 的函数，增加健壮性和日志 ---
def get_tenant_access_token(app_id: str, app_secret: str) -> Optional[str]:
    """
    使用 App ID 和 App Secret 获取 Tenant Access Token。

    Args:
        app_id (str): 应用的 App ID。
        app_secret (str): 应用的 App Secret。

    Returns:
        Optional[str]: 获取到的 Tenant Access Token，如果失败则返回 None。
    """
    # 创建 client
    client = lark.Client.builder() \
        .app_id(app_id) \
        .app_secret(app_secret) \
        .log_level(LogLevel.WARNING).build()

    # 构造请求对象
    request_body = InternalTenantAccessTokenRequestBody.builder() \
        .app_id(app_id) \
        .app_secret(app_secret) \
        .build()
    request: InternalTenantAccessTokenRequest = InternalTenantAccessTokenRequest.builder() \
        .request_body(request_body) \
        .build()

    # --- 新增：获取请求体字典用于打印 ---
    request_body_dict = {}
    if request.request_body:
        # lark-oapi 的 builder 对象通常有 to_dict 方法或可以通过 vars() 获取属性
        if hasattr(request.request_body, 'to_dict'):
             request_body_dict = request.request_body.to_dict()
        else:
             try:
                 # 尝试将 request_body 对象转为字典
                 request_body_dict = vars(request.request_body)
                 # 移除内部使用的下划线开头的属性（如果存在）
                 request_body_dict = {k: v for k, v in request_body_dict.items() if not k.startswith('_')}
             except TypeError:
                 logging.warning("Could not convert request body object to dict for logging.")
                 request_body_dict = {"error": "Could not serialize request body"}
    # --- 结束新增 ---

    try:
        response: InternalTenantAccessTokenResponse = client.auth.v3.tenant_access_token.internal(request)

        # 1. 检查请求是否成功 (code != 0)
        if not response.success():
            logging.error(
                f"Failed to get tenant_access_token, code: {response.code}, msg: {response.msg}, log_id: {response.get_log_id()}")
            # --- 新增：打印请求信息 ---
            endpoint_path = "/open-apis/auth/v3/tenant_access_token/internal"
            logging.error(f"Request Endpoint Path: {endpoint_path}")
            try:
                logging.error(f"Request Body: {json.dumps(request_body_dict, indent=2, ensure_ascii=False)}")
            except Exception as dump_err:
                 logging.error(f"Could not dump request body for logging: {dump_err}")
            # --- 结束新增 ---
            # 记录原始响应体以便调试
            if response.raw:
                 try:
                     raw_content = json.loads(response.raw.content)
                     logging.error(f"Raw response content on failure: {json.dumps(raw_content, indent=2, ensure_ascii=False)}")
                 except json.JSONDecodeError:
                     logging.error(f"Raw response content on failure (non-JSON): {response.raw.content.decode('utf-8', errors='ignore')}")
            return None

        # --- 修改：优先直接从 response 对象获取 token ---
        token = getattr(response, 'tenant_access_token', None)

        if token:
            logging.info("Successfully obtained tenant_access_token directly from response object.")
            return token
        else:
            # --- 新增：如果直接获取失败，尝试从原始响应解析 ---
            logging.warning("Could not get 'tenant_access_token' directly from response object. Attempting to parse raw response...")
            if response.raw and response.raw.content:
                try:
                    raw_data = json.loads(response.raw.content)
                    token = raw_data.get('tenant_access_token')
                    if token:
                        logging.info("Successfully obtained tenant_access_token by parsing raw response.")
                        return token
                    else:
                        logging.error("Parsed raw response, but 'tenant_access_token' key not found.")
                        logging.error(f"Raw response content (parsed): {json.dumps(raw_data, indent=2, ensure_ascii=False)}")
                except json.JSONDecodeError as json_err:
                    logging.error(f"Failed to parse raw response content as JSON: {json_err}")
                    logging.error(f"Raw response content (non-JSON): {response.raw.content.decode('utf-8', errors='ignore')}")
                except Exception as parse_err:
                    logging.error(f"An unexpected error occurred while parsing raw response: {parse_err}")
            else:
                logging.error("Raw response content is missing or empty, cannot parse for token.")
            # --- 结束新增 ---

            # 如果两种方式都失败了，记录详细信息
            logging.error("Failed to obtain tenant_access_token through both direct access and raw parsing.")
            try:
                logging.error(f"Final Response object attributes: {vars(response)}")
            except TypeError:
                 logging.error("Could not get attributes of the final response object.")
            return None
        # --- 结束修改 ---

    except Exception as e: # 保留通用异常捕获
        logging.error(f"An unexpected error occurred while getting tenant_access_token: {type(e).__name__} - {e}")
        import traceback
        logging.error(traceback.format_exc())
        # --- 新增：在异常时也尝试打印请求信息 ---
        endpoint_path = "/open-apis/auth/v3/tenant_access_token/internal"
        logging.error(f"Request Endpoint Path (during exception): {endpoint_path}")
        try:
            logging.error(f"Request Body (during exception): {json.dumps(request_body_dict, indent=2, ensure_ascii=False)}")
        except Exception as dump_err:
             logging.error(f"Could not dump request body during exception: {dump_err}")
        # --- 结束新增 ---
        return None

# --- 修改：移除 fields 参数，固定请求字段 ---
def fetch_bitable_records_with_token(
    app_token: str,
    table_id: str,
    view_id: str,
    bearer_token: str # 这个 token 现在可能是动态获取的
    # fields: Optional[List[str]] = None # <--- 移除此参数
) -> List[dict]:
    """
    使用 Bearer Token 从飞书多维表格获取记录 (通过 lark-oapi SDK, 使用 AppTableRecord)。
    固定只获取 '编号', 'round5', 'round10' 字段。
    """
    client = lark.Client.builder() \
        .enable_set_token(True) \
        .log_level(LogLevel.WARNING) \
        .build()

    all_items = []
    page_token: Optional[str] = None
    page_size = 100 # 可以根据需要调整，例如改为 20

    try:
        while True:
            # 2. 构建请求对象 (使用 ListAppTableRecordRequest)
            request_builder = ListAppTableRecordRequest.builder() \
                .app_token(app_token) \
                .table_id(table_id) \
                .view_id(view_id) \
                .page_size(page_size) \
                .user_id_type("open_id") \
                .field_names('["编号", "round5", "round10"]') # <--- 固定请求这三个字段

            if page_token:
                request_builder = request_builder.page_token(page_token)

            request: ListAppTableRecordRequest = request_builder.build()

            # 3. 发起请求 (使用 client.bitable.v1.app_table_record.list)
            logging.info(f"Fetching data from Feishu Bitable (SDK/Token/AppTable): App={app_token}, Table={table_id}, View={view_id}, PageToken={page_token}")
            # 假设 bearer_token 是 tenant_access_token
            option = lark.RequestOption.builder().tenant_access_token(bearer_token).build()
            response: ListAppTableRecordResponse = client.bitable.v1.app_table_record.list(request, option)

            # 4. 处理响应
            if response.success():
                data = response.data
                items = data.items if data and data.items else []
                all_items.extend(items)
                logging.info(f"Fetched {len(items)} items. Total items: {len(all_items)}")

                has_more = data.has_more if data else False
                page_token = data.page_token if data else None

                if not has_more or not page_token:
                    logging.info("No more pages to fetch.")
                    break
            else:
                logging.error(f"Feishu SDK Error: Code={response.code}, Msg={response.msg}")
                logging.error(f"Request ID: {response.get_request_id()}")
                if response.code == 99991661 or response.code == 10014:
                     logging.error("Feishu API Error: Missing or invalid access token.")
                break

    except lark.exception.ApiException as e: # <--- 修改：使用 lark.exception.ApiException
        logging.error(f"Lark SDK API Exception during Feishu fetch: code={e.code}, msg={e.msg}, log_id={e.log_id}")
    except Exception as e:
        logging.error(f"An unexpected error occurred during Feishu fetch (SDK/Token/AppTable): {e}")
        import traceback
        traceback.print_exc()

    # 转换格式
    result_list = []
    for item in all_items:
        record_dict = {
            "record_id": item.record_id,
            "fields": item.fields if item.fields else {}
        }
        result_list.append(record_dict)

    return result_list

# --- 写入记录到飞书多维表格的函数 ---
def write_records_to_bitable(
    app_token: str,
    table_id: str,
    records: List[Dict[str, Any]], # 记录列表，值可以是多种类型
    bearer_token: str # 需要传入认证 token
) -> bool:
    """
    将记录批量写入飞书多维表格。

    Args:
        app_token (str): 多维表格 App Token.
        table_id (str): 多维表格 Table ID.
        records (List[Dict[str, Any]]): 要写入的记录列表，每个字典代表一行，键是字段名。
        bearer_token (str): 用于认证的 Bearer Token (Tenant Access Token).

    Returns:
        bool: 写入是否成功。
    """
    if not records:
        logging.warning("No records provided to write to Feishu Bitable.")
        return True # 没有记录也算成功（无事可做）

    # 创建 client
    client = lark.Client.builder() \
        .enable_set_token(True) \
        .log_level(LogLevel.WARNING) \
        .build()

    # 准备请求体中的 records
    # lark SDK 需要 'fields' 键包含记录数据
    formatted_records = [{"fields": record} for record in records]

    # 构造请求对象
    request_body = BatchCreateAppTableRecordRequestBody.builder() \
        .records(formatted_records) \
        .build()
    request: BatchCreateAppTableRecordRequest = BatchCreateAppTableRecordRequest.builder() \
        .app_token(app_token) \
        .table_id(table_id) \
        .request_body(request_body) \
        .build()

    # 设置认证
    option = lark.RequestOption.builder().tenant_access_token(bearer_token).build() # 使用 tenant_access_token

    try:
        logging.info(f"Attempting to write {len(records)} records to Feishu Bitable: App={app_token}, Table={table_id}")
        response = client.bitable.v1.app_table_record.batch_create(request, option)

        # 处理失败返回
        if not response.success():
            logging.error(
                f"Failed to write records to Feishu Bitable, code: {response.code}, msg: {response.msg}, log_id: {response.get_log_id()}")
            # --- 修改：当错误码为 1254045 (FieldNameNotFound) 时，打印记录信息和所有尝试写入的字段名 ---
            if response.code == 1254045:
                logging.error("FieldNameNotFound error detected. One or more field names below might not exist in the target table.")
                try:
                    # 收集所有记录中使用的字段名
                    all_field_names = set()
                    for record in records:
                        if isinstance(record, dict):
                            all_field_names.update(record.keys())
                    logging.error(f"Field names attempted in this batch: {sorted(list(all_field_names))}")

                    # 仍然打印原始传入的 records 列表，以供详细检查
                    logging.error(f"Original records data being written: {json.dumps(records, indent=2, ensure_ascii=False)}")
                except Exception as dump_err:
                    logging.error(f"Could not dump records data or extract field names due to error: {dump_err}")
            # --- 结束修改 ---
            # 打印部分失败记录信息（如果可用）
            if hasattr(response, 'data') and response.data and hasattr(response.data, 'records'):
                 failed_records = [r.record_id for r in response.data.records if hasattr(r, 'record_id')] # 示例：获取可能失败的记录ID
                 logging.error(f"Failed record details (partial, may not be accurate for FieldNameNotFound): {failed_records}")
            return False

        # 处理业务结果
        logging.info(f"Successfully wrote {len(records)} records to Feishu Bitable.")
        # 可以检查 response.data.records 来确认写入的记录详情
        return True

    except Exception as e:
        logging.error(f"An unexpected error occurred during Feishu write: {e}")
        import traceback
        traceback.print_exc()
        return False

# --- 修改：移除 fields 参数 ---
def fetch_bitable_records(
    app_token: str,
    table_id: str,
    view_id: str
    # fields: Optional[List[str]] = None # <--- 移除此参数
) -> List[dict]:
    """
    从飞书多维表格获取记录 ('编号', 'round5', 'round10')。
    优先尝试使用 App ID/Secret 动态获取 Tenant Access Token。
    如果失败或未配置 App ID/Secret，则尝试使用环境变量中的 Bearer Token。
    """
    app_id = os.getenv("FEISHU_APP_ID")
    app_secret = os.getenv("FEISHU_APP_SECRET")
    bearer_token = None

    if app_id and app_secret:
        logging.info("Attempting to fetch tenant_access_token using App ID/Secret...")
        bearer_token = get_tenant_access_token(app_id, app_secret)
        if bearer_token:
            logging.info("Using dynamically obtained tenant_access_token for reading.")
        else:
            logging.warning("Failed to obtain tenant_access_token dynamically for reading. Falling back...")
    else:
        logging.info("App ID/Secret not configured for reading. Checking for Bearer Token in environment...")

    # 如果动态获取失败或未配置，尝试从环境变量获取
    if not bearer_token:
        bearer_token = os.getenv("FEISHU_BEARER_TOKEN")
        if bearer_token:
            logging.info("Using Bearer Token from environment variable for reading.")
        else:
            logging.error("Feishu authentication failed for reading: Neither dynamic token acquisition succeeded nor Bearer Token found.")
            return [] # 返回空列表表示读取失败

    # 使用获取到的 Token 调用读取函数
    # --- 修改：移除 fields 参数传递 ---
    raw_records = fetch_bitable_records_with_token(
        app_token=app_token,
        table_id=table_id,
        view_id=view_id,
        bearer_token=bearer_token
        # fields=fields # <--- 移除此参数传递
    )

    # 从原始记录中提取 'fields' 部分
    processed_records = [record.get('fields', {}) for record in raw_records if isinstance(record, dict)]
    return processed_records

# --- 新增：获取用于写入的 Token 的辅助函数 ---
# 这个函数是为了在写入时也能明确地获取 token，避免混淆
def get_write_token() -> Optional[str]:
    """获取用于写入操作的 Tenant Access Token"""
    app_id = os.getenv("FEISHU_APP_ID")
    app_secret = os.getenv("FEISHU_APP_SECRET")
    bearer_token = None

    if app_id and app_secret:
        logging.info("Attempting to fetch tenant_access_token using App ID/Secret for writing...")
        bearer_token = get_tenant_access_token(app_id, app_secret)
        if bearer_token:
            logging.info("Using dynamically obtained tenant_access_token for writing.")
        else:
            logging.warning("Failed to obtain tenant_access_token dynamically for writing. Falling back...")
    else:
        logging.info("App ID/Secret not configured for writing. Checking for Bearer Token in environment...")

    if not bearer_token:
        bearer_token = os.getenv("FEISHU_BEARER_TOKEN")
        if bearer_token:
            logging.info("Using Bearer Token from environment variable for writing.")
        else:
            logging.error("Feishu authentication failed for writing: Neither dynamic token acquisition succeeded nor Bearer Token found.")
            return None # 返回 None 表示写入认证失败
    return bearer_token