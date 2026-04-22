from tg_game.models import FeatureModule


MODULE = FeatureModule(
    key="diplomacy",
    name="外交",
    summary="查看宗门外交关系并记录示好、敌对、结盟等操作。",
    status="active",
    capabilities=["外交指令入口", "外交快照记录", "天下大势查询"],
    next_steps=["补外交版图字段", "区分友好/敌对/结盟状态", "联动宗门模块展示"],
)
