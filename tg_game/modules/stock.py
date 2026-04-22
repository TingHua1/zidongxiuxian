from tg_game.models import FeatureModule


MODULE = FeatureModule(
    key="stock",
    name="股市",
    summary="维护持仓、买卖点、趋势观察和风控阈值。",
    status="active",
    capabilities=[
        "持仓概览",
        "观察标的列表",
        "预警与止盈止损规则",
        "历史操作回看",
    ],
    next_steps=[
        "定义行情快照模型",
        "整理股市相关指令",
        "补交易策略引擎",
    ],
)
