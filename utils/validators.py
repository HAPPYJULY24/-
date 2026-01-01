"""
Input validation utilities for the application.
"""

from datetime import datetime


def validate_code(code: str, asset_type: str) -> tuple[bool, str]:
    """
    Validate asset code input.
    
    Args:
        code: Asset code entered by user
        asset_type: Type of asset
    
    Returns:
        Tuple of (is_valid, error_message)
        - If valid: (True, "")
        - If invalid: (False, "error message")
    """
    # 检查代码是否为空
    if not code or code.strip() == "":
        return False, "错误：请输入资产代码！\n\n例如：\n- 马股：1155\n- 美股：AAPL\n- 加密货币：BTC/USDT"
    
    # Strip whitespace
    code = code.strip()
    
    # Validate based on asset type
    if asset_type == "Malaysia Stock":
        # Malaysia stocks should be digits (will be appended with .KL)
        if not code.replace('.', '').isdigit():
            return False, "马股代码格式错误！\n\n请输入纯数字代码，例如：1155"
    
    elif asset_type == "US Stock":
        # US stocks should be alphabetic (can have dots)
        if not code.replace('.', '').isalpha():
            return False, "美股代码格式错误！\n\n请输入字母代码，例如：AAPL, MSFT, TSLA"
    
    elif asset_type == "Futures - Global":
        # 修改：期货代码允许等号、减号、点号（例如：GC=F, CL=F, ES-MAR23）
        import re
        if not re.match(r'^[A-Za-z0-9=\-.]+$', code):
            return False, "期货代码格式错误！\n\n请输入有效的期货代码\n例如：GC=F（黄金）, CL=F（原油）, SI=F（白银）"
    
    elif asset_type == "Crypto":
        # Crypto pairs should contain /
        if '/' not in code:
            return False, "加密货币格式错误！\n\n请使用 '/' 分隔交易对\n例如：BTC/USDT, ETH/MYR"
    
    return True, ""


def validate_date_range(start_date: datetime, end_date: datetime) -> tuple[bool, str]:
    """
    Validate date range.
    
    Args:
        start_date: Start date
        end_date: End date
    
    Returns:
        Tuple of (is_valid, error_message)
    """
    # 检查开始日期是否在结束日期之后
    if start_date >= end_date:
        return False, "日期范围错误！\n\n开始日期必须早于结束日期。\n\n当前设置：\n开始：{}\n结束：{}".format(
            start_date.strftime("%Y-%m-%d"),
            end_date.strftime("%Y-%m-%d")
        )
    
    # Check if start date is not in the future
    now = datetime.now()
    if start_date > now:
        return False, "开始日期不能晚于当前日期"

    # 检查日期范围是否过大（超过2年）
    delta = end_date - start_date
    if delta.days > 730:  # 约2年
        return False, "日期范围过大！\n\n建议选择不超过2年的数据范围。\n\n当前范围：{} 天".format(delta.days)
    
    return True, ""
