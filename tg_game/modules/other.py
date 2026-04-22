from tg_game.models import FeatureModule


MODULE = FeatureModule(
    key="other",
    name="其他玩法",
    summary="集中放卜筮问天、琉璃古塔、六道轮回盘、赌石和对赌类玩法。",
    status="active",
    capabilities=[
        "常用杂项玩法快捷入口",
        "古塔进度展示",
        "赌运与对赌命令面板",
        "历史消息样例参考",
    ],
    next_steps=[
        "补各玩法真实帮助回包",
        "补更多胜负统计",
        "补自动刷新与玩法冷却提示",
    ],
)
