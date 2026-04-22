from tg_game.models import FeatureModule


MODULE = FeatureModule(
    key="battle",
    name="战斗",
    summary="管理战斗准备、技能循环、目标选择和战报分析。",
    status="active",
    capabilities=[
        "战斗前检查清单",
        "技能和动作编排",
        "战报解析与统计",
        "冷却和资源判断",
    ],
    next_steps=[
        "定义战斗回合状态模型",
        "梳理常见战报正则",
        "设计自动战斗策略配置",
    ],
)
