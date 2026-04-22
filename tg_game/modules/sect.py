from tg_game.models import FeatureModule


MODULE = FeatureModule(
    key="sect",
    name="宗门",
    summary="管理宗门信息、点卯传功、捐献贡献、悬赏任务和各宗门专属玩法。",
    status="active",
    capabilities=[
        "我的宗门状态快照",
        "点卯、传功、捐献与贡献记录",
        "宗门悬赏与任务提醒",
        "宗门专属玩法入口",
    ],
    next_steps=[
        "接入黄枫谷、星宫、凌霄宫等宗门专属解析",
        "补宗门点卯/传功/悬赏自动链",
        "增加宗门贡献与宝库兑换策略",
    ],
)
