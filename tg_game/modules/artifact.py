from tg_game.models import FeatureModule


MODULE = FeatureModule(
    key="artifact",
    name="法宝",
    summary="管理法宝状态、耐久、修理与器灵相关玩法。",
    status="active",
    capabilities=["法宝状态查询", "修理入口", "器灵快照"],
    next_steps=["补法宝耐久解析", "补器灵等级字段", "联动高阶炼制路线"],
)
