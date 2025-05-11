import logging
import json
import os
from typing import List, Optional, Dict, Any, Union

# 导入 FastMCP 相关模块
from fastmcp import FastMCP, Context

# 导入 lark-oapi 相关模块
import lark_oapi as lark
from lark_oapi import LogLevel # 导入日志级别
# 更新导入，使用 AppTableRecord 相关的类 (读取)
from lark_oapi.api.bitable.v1 import ListAppTableRecordRequest, ListAppTableRecordResponse
# 导入：用于获取 tenant_access_token
from lark_oapi.api.auth.v3 import InternalTenantAccessTokenRequest, InternalTenantAccessTokenRequestBody, InternalTenantAccessTokenResponse
# 导入：用于写入多维表格记录
from lark_oapi.api.bitable.v1 import BatchCreateAppTableRecordRequest, BatchCreateAppTableRecordRequestBody, AppTableRecord

# 配置日志
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# 创建 MCP 服务器
mcp = FastMCP("飞书多维表格MCP服务器")

# ======== 实现 MCP 工具和资源 ========

@mcp.tool()
async def get_tenant_access_token(app_id: str, app_secret: str, ctx: Context) -> Dict[str, Any]:
    """
    使用 App ID 和 App Secret 获取飞书的 Tenant Access Token。
    
    Args:
        app_id: 应用的 App ID
        app_secret: 应用的 App Secret
        
    Returns:
        包含 token 和状态信息的字典
    """
    await ctx.info(f"正在获取 Tenant Access Token，App ID: {app_id[:4]}***")
    
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

    try:
        response: InternalTenantAccessTokenResponse = client.auth.v3.tenant_access_token.internal(request)

        # 检查请求是否成功
        if not response.success():
            error_msg = f"获取 tenant_access_token 失败，code: {response.code}, msg: {response.msg}, log_id: {response.get_log_id()}"
            await ctx.error(error_msg)
            return {"success": False, "error": error_msg}

        # 优先直接从 response 对象获取 token
        token = getattr(response, 'tenant_access_token', None)

        if token:
            await ctx.info("成功从 response 对象直接获取 tenant_access_token")
            return {"success": True, "token": token}
        else:
            # 如果直接获取失败，尝试从原始响应解析
            await ctx.warning("无法直接从 response 对象获取 'tenant_access_token'，尝试解析原始响应...")
            if response.raw and response.raw.content:
                try:
                    raw_data = json.loads(response.raw.content)
                    token = raw_data.get('tenant_access_token')
                    if token:
                        await ctx.info("通过解析原始响应成功获取 tenant_access_token")
                        return {"success": True, "token": token}
                    else:
                        error_msg = "已解析原始响应，但未找到 'tenant_access_token' 键"
                        await ctx.error(error_msg)
                        return {"success": False, "error": error_msg}
                except Exception as parse_err:
                    error_msg = f"解析原始响应时发生错误: {parse_err}"
                    await ctx.error(error_msg)
                    return {"success": False, "error": error_msg}
            else:
                error_msg = "原始响应内容缺失或为空，无法解析 token"
                await ctx.error(error_msg)
                return {"success": False, "error": error_msg}

    except Exception as e:
        error_msg = f"获取 tenant_access_token 时发生意外错误: {type(e).__name__} - {e}"
        await ctx.error(error_msg)
        return {"success": False, "error": error_msg}

@mcp.tool()
async def fetch_bitable_records(
    app_token: str, 
    table_id: str, 
    view_id: str, 
    bearer_token: Optional[str] = None, 
    ctx: Context = None
) -> Dict[str, Any]:
    """
    从飞书多维表格获取记录。
    
    Args:
        app_token: 多维表格的 App Token
        table_id: 表格 ID
        view_id: 视图 ID
        bearer_token: Bearer Token（可选，如果不提供将尝试从环境变量或自动获取）
        
    Returns:
        包含记录数据和状态信息的字典
    """
    if ctx:
        await ctx.info(f"正在从飞书多维表格获取记录: App={app_token}, Table={table_id}, View={view_id}")
    
    # 如果未提供 token，尝试获取
    if not bearer_token:
        app_id = os.getenv("FEISHU_APP_ID")
        app_secret = os.getenv("FEISHU_APP_SECRET")
        
        if app_id and app_secret and ctx:
            await ctx.info("正在使用 App ID/Secret 获取 tenant_access_token...")
            token_result = await get_tenant_access_token(app_id, app_secret, ctx)
            if token_result["success"]:
                bearer_token = token_result["token"]
                await ctx.info("已获取动态 token 用于读取操作")
            else:
                await ctx.warning("无法动态获取 token，尝试使用环境变量...")
        
        # 如果动态获取失败或未配置，尝试从环境变量获取
        if not bearer_token:
            bearer_token = os.getenv("FEISHU_BEARER_TOKEN")
            if bearer_token and ctx:
                await ctx.info("正在使用环境变量中的 Bearer Token 进行读取操作")
            elif ctx:
                error_msg = "无法进行飞书认证：无法获取 token，也未找到环境变量中的 Bearer Token"
                await ctx.error(error_msg)
                return {"success": False, "error": error_msg, "records": []}

    client = lark.Client.builder() \
        .enable_set_token(True) \
        .log_level(LogLevel.WARNING) \
        .build()

    all_items = []
    page_token: Optional[str] = None
    page_size = 100  # 可以根据需要调整

    try:
        while True:
            # 构建请求对象
            request_builder = ListAppTableRecordRequest.builder() \
                .app_token(app_token) \
                .table_id(table_id) \
                .view_id(view_id) \
                .page_size(page_size) \
                .user_id_type("open_id") \
                .field_names('["编号", "round5", "round10"]')  # 固定请求这三个字段

            if page_token:
                request_builder = request_builder.page_token(page_token)

            request: ListAppTableRecordRequest = request_builder.build()

            # 发起请求
            if ctx:
                await ctx.info(f"正在从飞书多维表格获取数据: App={app_token}, Table={table_id}, PageToken={page_token}")
            
            # 使用提供的 bearer_token
            option = lark.RequestOption.builder().tenant_access_token(bearer_token).build()
            response: ListAppTableRecordResponse = client.bitable.v1.app_table_record.list(request, option)

            # 处理响应
            if response.success():
                data = response.data
                items = data.items if data and data.items else []
                all_items.extend(items)
                if ctx:
                    await ctx.info(f"已获取 {len(items)} 条记录。总记录数: {len(all_items)}")

                has_more = data.has_more if data else False
                page_token = data.page_token if data else None

                if not has_more or not page_token:
                    if ctx:
                        await ctx.info("没有更多页面需要获取")
                    break
            else:
                error_msg = f"飞书 SDK 错误: Code={response.code}, Msg={response.msg}"
                if ctx:
                    await ctx.error(error_msg)
                    await ctx.error(f"请求 ID: {response.get_request_id()}")
                
                if response.code == 99991661 or response.code == 10014:
                    if ctx:
                        await ctx.error("飞书 API 错误：缺少或无效的访问令牌")
                
                return {"success": False, "error": error_msg, "records": []}

    except lark.exception.ApiException as e:
        error_msg = f"飞书获取过程中的 Lark SDK API 异常: code={e.code}, msg={e.msg}, log_id={e.log_id}"
        if ctx:
            await ctx.error(error_msg)
        return {"success": False, "error": error_msg, "records": []}
    
    except Exception as e:
        error_msg = f"飞书获取过程中发生意外错误: {e}"
        if ctx:
            await ctx.error(error_msg)
        return {"success": False, "error": error_msg, "records": []}

    # 转换记录格式
    processed_records = []
    for item in all_items:
        record_dict = item.fields if item.fields else {}
        processed_records.append(record_dict)
        
    return {"success": True, "records": processed_records}

@mcp.tool()
async def write_records_to_bitable(
    app_token: str,
    table_id: str,
    records: List[Dict[str, Any]],
    bearer_token: Optional[str] = None,
    ctx: Context = None
) -> Dict[str, Any]:
    """
    将记录写入到飞书多维表格。
    
    Args:
        app_token: 多维表格的 App Token
        table_id: 表格 ID
        records: 要写入的记录列表
        bearer_token: Bearer Token（可选，如果不提供将尝试从环境变量或自动获取）
        
    Returns:
        包含操作结果和状态信息的字典
    """
    if not records:
        if ctx:
            await ctx.warning("没有提供要写入到飞书多维表格的记录")
        return {"success": True, "message": "没有记录需要写入"}

    if ctx:
        await ctx.info(f"正在尝试写入 {len(records)} 条记录到飞书多维表格")

    # 如果未提供 token，尝试获取
    if not bearer_token:
        app_id = os.getenv("FEISHU_APP_ID")
        app_secret = os.getenv("FEISHU_APP_SECRET")
        
        if app_id and app_secret and ctx:
            await ctx.info("正在使用 App ID/Secret 获取用于写入的 tenant_access_token...")
            token_result = await get_tenant_access_token(app_id, app_secret, ctx)
            if token_result["success"]:
                bearer_token = token_result["token"]
                await ctx.info("已获取动态 token 用于写入操作")
            else:
                await ctx.warning("无法动态获取 token，尝试使用环境变量...")
        
        # 如果动态获取失败或未配置，尝试从环境变量获取
        if not bearer_token:
            bearer_token = os.getenv("FEISHU_BEARER_TOKEN")
            if bearer_token and ctx:
                await ctx.info("正在使用环境变量中的 Bearer Token 进行写入操作")
            elif ctx:
                error_msg = "无法进行飞书认证：无法获取 token，也未找到环境变量中的 Bearer Token"
                await ctx.error(error_msg)
                return {"success": False, "error": error_msg}

    # 创建 client
    client = lark.Client.builder() \
        .enable_set_token(True) \
        .log_level(LogLevel.WARNING) \
        .build()

    # 准备请求体中的 records
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
    option = lark.RequestOption.builder().tenant_access_token(bearer_token).build()

    try:
        if ctx:
            await ctx.info(f"正在尝试写入 {len(records)} 条记录到飞书多维表格: App={app_token}, Table={table_id}")
        
        response = client.bitable.v1.app_table_record.batch_create(request, option)

        # 处理失败返回
        if not response.success():
            error_msg = f"写入记录到飞书多维表格失败: code={response.code}, msg={response.msg}, log_id={response.get_log_id()}"
            if ctx:
                await ctx.error(error_msg)
                
                # 当错误码为 1254045 (FieldNameNotFound) 时，打印记录信息和所有尝试写入的字段名
                if response.code == 1254045:
                    await ctx.error("检测到 FieldNameNotFound 错误。下列一个或多个字段名可能不存在于目标表中。")
                    try:
                        # 收集所有记录中使用的字段名
                        all_field_names = set()
                        for record in records:
                            if isinstance(record, dict):
                                all_field_names.update(record.keys())
                        await ctx.error(f"此批次尝试的字段名: {sorted(list(all_field_names))}")
                    except Exception as dump_err:
                        await ctx.error(f"由于错误无法转储记录数据或提取字段名: {dump_err}")
            
            return {"success": False, "error": error_msg}

        # 处理业务结果
        success_msg = f"成功写入 {len(records)} 条记录到飞书多维表格"
        if ctx:
            await ctx.info(success_msg)
        
        return {"success": True, "message": success_msg}

    except Exception as e:
        error_msg = f"飞书写入过程中发生意外错误: {e}"
        if ctx:
            await ctx.error(error_msg)
        
        return {"success": False, "error": error_msg}

# 添加系统资源，返回版本信息
@mcp.resource("feishu://info")
def get_feishu_info() -> str:
    """获取飞书MCP服务器信息"""
    return json.dumps({
        "name": "飞书多维表格MCP服务器",
        "version": "1.0.0",
        "description": "为飞书多维表格提供MCP服务的服务器",
        "date": "2025-04-23"
    }, ensure_ascii=False)

# 如果直接运行此文件，启动MCP服务器
if __name__ == "__main__":
    # 默认使用stdio传输
    mcp.run(transport="stdio") 