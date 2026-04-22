from tg_game.models import FeatureModule


MODULE = FeatureModule(
    key="estate",
    name="洞府",
    summary="管理洞府状态、灵脉静室升级和访客相关玩法。",
    status="active",
    capabilities=["洞府状态入口", "灵脉静室升级命令", "访客快照"],
    next_steps=["补洞府等级字段", "补访客事件解析", "联动侍妾与展示台"],
)
