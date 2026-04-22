from tg_game.models import FeatureModule


MODULE = FeatureModule(
    key="inventory",
    name="物品管理",
    summary="管理背包、仓库、丹药、装备和可消耗资源。",
    status="active",
    capabilities=[
        "物品清单视图",
        "稀有度和分类筛选",
        "自动使用/出售规则",
        "容量与整理提醒",
    ],
    next_steps=[
        "定义物品实体字段",
        "接入背包文本解析",
        "补自动整理动作",
    ],
)
