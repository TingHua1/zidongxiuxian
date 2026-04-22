from tg_game.models import FeatureModule


MODULE = FeatureModule(
    key="cultivation",
    name="修炼",
    summary="管理普通闭关、深度闭关、修为结算、冷却与闭关自动续跑。",
    status="active",
    capabilities=[
        "普通闭关自动循环",
        "深度闭关状态探测与续跑",
        "修为收益、境界、进度记录",
        "冷却与轮询调度",
    ],
    next_steps=[
        "补充突破与闭关衍生玩法",
        "接入更细的修炼策略配置",
        "增加收益趋势与统计看板",
    ],
)
