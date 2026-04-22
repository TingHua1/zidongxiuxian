from tg_game.models import FeatureModule


MODULE = FeatureModule(
    key="dungeon",
    name="副本",
    summary="处理副本进入条件、次数、阶段流程和掉落统计。",
    status="active",
    capabilities=[
        "副本开放状态面板",
        "次数和体力追踪",
        "掉落统计",
        "队伍/单刷配置入口",
    ],
    next_steps=[
        "定义副本流程节点",
        "整理副本文案匹配规则",
        "补自动刷本执行链",
    ],
)
