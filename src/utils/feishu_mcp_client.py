import asyncio
import json
import logging
import os
from typing import List, Dict, Any, Optional

from fastmcp import Client
from fastmcp.client.stdio import StdioServerParameters

# 配置日志
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class FeishuMCPClient:
    """飞书 MCP 客户端"""
    
    def __init__(self, server_path: str = "src/utils/feishu_mcp_server.py"):
        """
        初始化飞书 MCP 客户端
        
        Args:
            server_path: MCP 服务器脚本的路径
        """
        self.server_path = server_path
        self.server_params = StdioServerParameters(
            command="python",
            args=[server_path],
            env=os.environ.copy()  # 传递当前环境变量
        )
    
    async def fetch_records(
        self, 
        app_token: str, 
        table_id: str, 
        view_id: str, 
        bearer_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        从飞书多维表格获取记录
        
        Args:
            app_token: 多维表格的 App Token
            table_id: 表格 ID
            view_id: 视图 ID
            bearer_token: 可选的 Bearer Token
            
        Returns:
            包含记录和状态信息的字典
        """
        logger.info(f"正在从飞书多维表格获取记录: App={app_token}, Table={table_id}")
        
        arguments = {
            "app_token": app_token,
            "table_id": table_id,
            "view_id": view_id
        }
        
        if bearer_token:
            arguments["bearer_token"] = bearer_token
        
        try:
            # 连接到 MCP 服务器
            async with Client(self.server_params) as client:
                # 初始化客户端
                await client.initialize()
                
                # 列出可用工具
                tools = await client.list_tools()
                logger.debug(f"可用的工具: {[tool.name for tool in tools.tools]}")
                
                # 调用获取记录的工具
                result = await client.call_tool("fetch_bitable_records", arguments)
                
                # 解析结果
                if result.content and result.content[0].text:
                    try:
                        return json.loads(result.content[0].text)
                    except json.JSONDecodeError:
                        logger.error(f"无法解析返回的JSON: {result.content[0].text}")
                        return {"success": False, "error": "JSON解析错误", "records": []}
                else:
                    logger.error("从MCP服务器返回的内容为空")
                    return {"success": False, "error": "返回内容为空", "records": []}
                    
        except Exception as e:
            logger.error(f"与MCP服务器通信时发生错误: {e}")
            return {"success": False, "error": str(e), "records": []}
    
    async def write_records(
        self, 
        app_token: str, 
        table_id: str, 
        records: List[Dict[str, Any]], 
        bearer_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        将记录写入到飞书多维表格
        
        Args:
            app_token: 多维表格的 App Token
            table_id: 表格 ID
            records: 要写入的记录列表
            bearer_token: 可选的 Bearer Token
            
        Returns:
            包含操作结果和状态信息的字典
        """
        logger.info(f"正在写入 {len(records)} 条记录到飞书多维表格: App={app_token}, Table={table_id}")
        
        arguments = {
            "app_token": app_token,
            "table_id": table_id,
            "records": records
        }
        
        if bearer_token:
            arguments["bearer_token"] = bearer_token
        
        try:
            # 连接到 MCP 服务器
            async with Client(self.server_params) as client:
                # 初始化客户端
                await client.initialize()
                
                # 调用写入记录的工具
                result = await client.call_tool("write_records_to_bitable", arguments)
                
                # 解析结果
                if result.content and result.content[0].text:
                    try:
                        return json.loads(result.content[0].text)
                    except json.JSONDecodeError:
                        logger.error(f"无法解析返回的JSON: {result.content[0].text}")
                        return {"success": False, "error": "JSON解析错误"}
                else:
                    logger.error("从MCP服务器返回的内容为空")
                    return {"success": False, "error": "返回内容为空"}
                    
        except Exception as e:
            logger.error(f"与MCP服务器通信时发生错误: {e}")
            return {"success": False, "error": str(e)}
    
    async def get_server_info(self) -> Dict[str, Any]:
        """
        获取服务器信息
        
        Returns:
            服务器信息字典
        """
        try:
            # 连接到 MCP 服务器
            async with Client(self.server_params) as client:
                # 初始化客户端
                await client.initialize()
                
                # 列出可用资源
                resources = await client.list_resources()
                logger.debug(f"可用的资源: {[res.uri for res in resources.resources]}")
                
                # 读取服务器信息资源
                content, mime_type = await client.read_resource("feishu://info")
                
                # 解析返回的JSON
                if content:
                    try:
                        return json.loads(content)
                    except json.JSONDecodeError:
                        logger.error(f"无法解析返回的JSON: {content}")
                        return {"error": "JSON解析错误"}
                else:
                    return {"error": "返回内容为空"}
                    
        except Exception as e:
            logger.error(f"获取服务器信息时发生错误: {e}")
            return {"error": str(e)}

# 示例用法
async def main():
    """示例用法"""
    client = FeishuMCPClient()
    
    # 获取服务器信息
    info = await client.get_server_info()
    print(f"服务器信息: {json.dumps(info, ensure_ascii=False, indent=2)}")
    
    # 读取记录示例
    app_token = os.getenv("FEISHU_APP_TOKEN", "your_app_token")
    table_id = os.getenv("FEISHU_TABLE_ID", "your_table_id")
    view_id = os.getenv("FEISHU_VIEW_ID", "your_view_id")
    
    result = await client.fetch_records(app_token, table_id, view_id)
    
    if result["success"]:
        print(f"成功获取 {len(result['records'])} 条记录")
        # 打印前3条记录
        for i, record in enumerate(result["records"][:3]):
            print(f"记录 {i+1}: {json.dumps(record, ensure_ascii=False)}")
    else:
        print(f"获取记录失败: {result.get('error', '未知错误')}")
    
    # 写入记录示例
    records_to_write = [
        {"编号": 100, "round5": "测试记录1", "round10": "MCP写入测试1"},
        {"编号": 101, "round5": "测试记录2", "round10": "MCP写入测试2"}
    ]
    
    write_result = await client.write_records(app_token, table_id, records_to_write)
    
    if write_result["success"]:
        print(f"写入记录成功: {write_result.get('message', '')}")
    else:
        print(f"写入记录失败: {write_result.get('error', '未知错误')}")

if __name__ == "__main__":
    asyncio.run(main()) 