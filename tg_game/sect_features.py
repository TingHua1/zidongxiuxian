def _text_field(name: str, label: str, placeholder: str) -> dict:
    return {
        "name": name,
        "label": label,
        "type": "text",
        "placeholder": placeholder,
        "required": True,
    }


def _select_field(name: str, label: str, options: list[tuple[str, str]]) -> dict:
    return {
        "name": name,
        "label": label,
        "type": "select",
        "required": True,
        "options": [
            {"value": value, "label": option_label} for value, option_label in options
        ],
    }


def _button_action(key: str, title: str, command: str, description: str) -> dict:
    return {
        "key": key,
        "title": title,
        "command": command,
        "description": description,
        "submit_label": "加入队列",
        "fields": [],
    }


def _form_action(
    key: str,
    title: str,
    template: str,
    description: str,
    fields: list[dict],
) -> dict:
    return {
        "key": key,
        "title": title,
        "template": template,
        "description": description,
        "submit_label": "生成指令并入队",
        "fields": fields,
    }


SECT_FEATURES = [
    {
        "name": "黄枫谷",
        "summary": "药园种植与丹道路线，适合新人积累贡献和基础材料。",
        "detail": "以前期养成为主，小药园需要持续播种、除草、除虫和浇水，稳定产凝血草、清灵草、种子和贡献，适合新人走丹药与基础材料线。",
        "playbook": [
            "先看药园状态",
            "按地块播种并处理杂草、害虫、干涸",
            "成熟后采药，资源够了再扩建药园",
        ],
        "actions": [
            _button_action(
                "garden",
                "查看小药园",
                ".小药园",
                "先看每块灵田的成熟、杂草、害虫和干涸状态。",
            ),
            _form_action(
                "sow",
                "播种",
                ".播种 {plot} {seed}",
                "按地块播下目标种子，适合日常补种。",
                [
                    _text_field("plot", "地块", "例如 1"),
                    _text_field("seed", "种子", "例如 清灵草种子"),
                ],
            ),
            _form_action(
                "harvest",
                "采药",
                ".采药 {plot}",
                "成熟后按地块采药。",
                [_text_field("plot", "地块", "例如 1")],
            ),
            _form_action(
                "weed",
                "除草",
                ".除草 {plot}",
                "地块长草时直接处理。",
                [_text_field("plot", "地块", "例如 1")],
            ),
            _form_action(
                "bug",
                "除虫",
                ".除虫 {plot}",
                "地块生虫时直接处理。",
                [_text_field("plot", "地块", "例如 1")],
            ),
            _form_action(
                "water",
                "浇水",
                ".浇水 {plot}",
                "地块干涸时补水保产量。",
                [_text_field("plot", "地块", "例如 1")],
            ),
            _button_action(
                "expand", "扩建药园", ".扩建药园", "药园稳定后继续扩容，提高长期产出。"
            ),
        ],
        "commands": [
            ".小药园",
            ".播种",
            ".采药",
            ".除草",
            ".除虫",
            ".浇水",
            ".扩建药园",
            ".晋升长老",
        ],
        "notes": ["新人友好", "灵田维护型玩法", "可提前准备九转解厄丹路线"],
    },
    {
        "name": "太一门",
        "summary": "围绕引道和神识强化修炼与斗法的过渡型宗门。",
        "detail": "围绕引道攒神识吃十二时辰增益，引水偏闭关、引火偏斗法，长期收益是神识转永久闭关成功率，属于修炼和斗法双向过渡线。",
        "playbook": [
            "先开引道拿十二时辰增益",
            "闭关线优先引水，斗法线优先引火",
            "神识积累起来后再打神识冲击",
        ],
        "actions": [
            _form_action(
                "guide",
                "引道",
                ".引道 {element}",
                "按当前目标选择五行引道，闭关常用水，斗法常用火。",
                [
                    _select_field(
                        "element",
                        "五行",
                        [
                            ("金", "金"),
                            ("木", "木"),
                            ("水", "水"),
                            ("火", "火"),
                            ("土", "土"),
                        ],
                    )
                ],
            ),
            _button_action(
                "shock", "神识冲击", ".神识冲击", "神识够用时压对手闭关成功率。"
            ),
        ],
        "commands": [
            ".引道 金",
            ".引道 木",
            ".引道 水",
            ".引道 火",
            ".引道 土",
            ".神识冲击",
        ],
        "notes": ["水引道适合闭关", "火引道适合斗法", "神识可叠闭关成功率"],
    },
    {
        "name": "星宫",
        "summary": "侍妾、启阵和观星台是核心，适合修炼与资源双成长。",
        "detail": "侍妾、启阵和观星台联动明显，既能靠星力加持放大闭关收益，也能通过观星和日常俸禄拿灵石与定向材料，整体收益高但更吃协作和配置。",
        "playbook": [
            "先看侍妾和观星台状态",
            "启阵或助阵挂星力加持",
            "观星后按编号牵引星辰并收集精华",
        ],
        "actions": [
            _button_action(
                "companion", "我的侍妾", ".我的侍妾", "先确认侍妾和情缘状态。"
            ),
            _button_action("matrix", "启阵", ".启阵", "开阵吃星力加持。"),
            _button_action("assist", "助阵", ".助阵", "协助阵法，补队伍联动。"),
            _button_action(
                "starboard", "观星台", ".观星台", "查看当前星辰盘面与可操作槽位。"
            ),
            _button_action("divine", "观星", ".观星", "每日基础观星动作。"),
            _form_action(
                "pull",
                "牵引星辰",
                ".牵引星辰 {slot} {star}",
                "按槽位和目标星辰做定向牵引。",
                [
                    _text_field("slot", "编号", "例如 1"),
                    _text_field("star", "星辰", "例如 天雷星"),
                ],
            ),
            _form_action(
                "collect",
                "收集精华",
                ".收集精华 {slot}",
                "按编号收取可用精华。",
                [_text_field("slot", "编号", "例如 1")],
            ),
        ],
        "commands": [
            ".我的侍妾",
            ".每日问安",
            ".启阵",
            ".助阵",
            ".观星",
            ".观星台",
            ".牵引星辰",
            ".收集精华",
        ],
        "notes": [
            "启阵强力联动深度闭关",
            "庚金星和天雷星常用",
            "情缘值 500 有明显收益",
        ],
    },
    {
        "name": "凌霄宫",
        "summary": "登天阶与罡风淬体的长期养成宗门。",
        "detail": "登天阶、问心、罡风和借势是一条长期养成线，核心回报是周天奖励与战力加成，后期能稳定摸到灵眼之树、三级妖丹、养魂木等高阶主材。",
        "playbook": [
            "先看凌霄宫与天阶状态",
            "按冷却登天阶，问心和借势保持在线",
            "周天稳定后再补罡风淬体",
        ],
        "actions": [
            _button_action(
                "overview", "凌霄宫概览", ".凌霄宫", "先拉取整页凌霄宫状态。"
            ),
            _button_action("status", "天阶状态", ".天阶状态", "查看云阶、周天和冷却。"),
            _button_action(
                "mind", "问心台", ".问心台", "补问心状态，决定后续登阶稳定性。"
            ),
            _button_action("step", "登天阶", ".登天阶", "到点直接登阶。"),
            _button_action("wind", "引九天罡风", ".引九天罡风", "补罡风淬体层数。"),
            _button_action(
                "gate", "借天门势", ".借天门势", "给后续登阶或战力成长挂额外势能。"
            ),
        ],
        "commands": [
            ".天阶状态",
            ".问心台",
            ".登天阶",
            ".引九天罡风",
            ".借天门势",
            ".晋升长老",
        ],
        "notes": ["更偏长期成长", "云阶和周天奖励是主线", "冷却和问心状态都要跟踪"],
    },
    {
        "name": "合欢宗",
        "summary": "双修、心印和互动路线，偏社交和特殊收益。",
        "detail": "以双修、同参和心印互动为核心，稳定产修为与宗门贡献，配合合欢散和进阶双修法门还有机会摸到额外图纸，更适合有固定互动对象时发力。",
        "playbook": [
            "先确认同参和心印关系",
            "根据目标状态切温养或采补",
            "结束后补结印和贡献收益",
        ],
        "actions": [
            _button_action("dual", "闭关双修", ".闭关双修", "进入双修主流程。"),
            _button_action("companion", "缔结同参", ".缔结同参", "先确认同参关系。"),
            _button_action(
                "warm", "双修温养", ".双修 温养", "偏稳定修为与贡献的常用路线。"
            ),
            _button_action(
                "harvest", "双修采补", ".双修 采补", "收益更激进时再走采补。"
            ),
            _button_action(
                "imprint", "种下心印", ".种下心印", "补心印进度，为后续互动做准备。"
            ),
            _button_action("seal", "结印", ".结印", "完成互动后收尾。"),
        ],
        "commands": [
            ".闭关双修",
            ".缔结同参",
            ".双修 温养",
            ".种下心印",
            ".双修 采补",
            ".结印",
        ],
        "notes": ["更偏互动玩法", "不适合完全零基础起手", "需要配合目标状态"],
    },
    {
        "name": "万灵宗",
        "summary": "灵兽培养和出战体系，偏战力成长与材料获取。",
        "detail": "灵兽培养是主线，放养、出战和探渊会持续带回妖丹、兽血、灵草等材料，同时还能补战力成长，属于偏资源回收和副产线的宗门。",
        "playbook": ["先看灵兽状态", "放养和探渊补材料", "需要战力时再安排喂养和出战"],
        "actions": [
            _button_action("search", "寻觅灵兽", ".寻觅灵兽", "还没成型时先补新灵兽。"),
            _button_action(
                "status", "我的灵兽", ".我的灵兽", "查看灵兽现状、放养和出战信息。"
            ),
            _button_action(
                "farm", "一键放养", ".一键放养", "日常回收材料最省心的动作。"
            ),
            _button_action("abyss", "探渊", ".探渊", "补副产资源和额外掉落。"),
            _form_action(
                "battle",
                "灵兽出战",
                ".灵兽出战 {beast}",
                "按灵兽名派出作战。",
                [_text_field("beast", "灵兽", "例如 小青")],
            ),
            _form_action(
                "feed",
                "喂养灵兽",
                ".喂养 {beast} {item_bundle}",
                "按灵兽名和物品数量格式投喂。",
                [
                    _text_field("beast", "灵兽", "例如 小青"),
                    _text_field("item_bundle", "物品*数量", "例如 灵兽口粮*10"),
                ],
            ),
        ],
        "commands": [
            ".寻觅灵兽",
            ".我的灵兽",
            ".喂养",
            ".灵兽出战",
            ".一键放养",
            ".灵兽偷菜",
            ".探渊",
        ],
        "notes": ["后期价值较高", "风雷翅可缩短部分冷却", "不是纯修为宗门"],
    },
    {
        "name": "落云宗",
        "summary": "灵树养成和协同守山玩法，考验时机与协作。",
        "detail": "灵树环境与守山节奏决定收益，日常要盯灵纹、环境和来袭事件，核心争夺是树枝与灵眼之树这类高阶主材，适合能跟进时段和协同的玩家。",
        "playbook": [
            "先看灵树状态和环境倾向",
            "根据环境及时灌溉",
            "来袭时优先协同守山，再安排采摘灵果",
        ],
        "actions": [
            _button_action(
                "status", "灵树状态", ".灵树状态", "先确认环境、灵纹和本轮产出。"
            ),
            _button_action(
                "water", "灵树灌溉", ".灵树灌溉", "灵树状态不佳时及时补灌。"
            ),
            _button_action(
                "guard", "协同守山", ".协同守山", "遇到来袭事件时优先守山。"
            ),
            _button_action(
                "harvest", "采摘灵果", ".采摘灵果", "状态允许时收割当前果实。"
            ),
        ],
        "commands": [".灵树状态", ".灵树灌溉", ".协同守山", ".采摘灵果"],
        "notes": ["环境变化影响操作", "古剑门来袭时守山优先", "更适合有一定基础后加入"],
    },
    {
        "name": "阴罗宗",
        "summary": "阴罗幡、炼魂和诅咒路线，资源循环复杂。",
        "detail": "围绕阴罗幡、魂魄和煞气循环展开，前期管理重也更容易挨打，但煞气池和戾魄体系成型后能稳定转出高价值资源，属于高风险高回报路线。",
        "playbook": [
            "先看阴罗幡和煞气池",
            "献祭和化功先把循环跑起来",
            "成型后再靠血洗和收割放大利润",
        ],
        "actions": [
            _button_action(
                "banner", "我的阴罗幡", ".我的阴罗幡", "先确认阴罗幡和魂魄储备。"
            ),
            _button_action(
                "upgrade", "升级阴罗幡", ".升级阴罗幡", "材料够时优先补幡。"
            ),
            _button_action(
                "sacrifice", "每日献祭", ".每日献祭", "稳定补煞气池的基础动作。"
            ),
            {
                "key": "convert",
                "title": "化煞",
                "template": ".化功为煞 {amount}",
                "description": "输入要转化的修为数量，按 5:1 比例转成煞气。",
                "submit_label": "化煞",
                "fields": [
                    _text_field("amount", "修为数量", "例如 5000"),
                ],
            },
            _button_action("hunt", "血洗山林", ".血洗山林", "补魂魄和额外煞气收益。"),
            _button_action("harvest", "收割", ".收割", "成型后收割循环收益。"),
        ],
        "commands": [
            ".我的阴罗幡",
            ".升级阴罗幡",
            ".每日献祭",
            ".化功为煞",
            ".血洗山林",
            ".召唤魔影",
            ".囚禁魂魄",
            ".下咒",
            ".血咒四方",
            ".收割",
        ],
        "notes": ["不太适合新人", "偏侵略和体系循环", "高收益但管理复杂"],
    },
    {
        "name": "元婴宗",
        "summary": "元婴出窍、问道和功法参悟，属于高阶宗门。",
        "detail": "元婴出窍、问道和元婴闭关都偏高阶长线，日常既能拿大量修为，也能稳定带回灵石、妖丹和功法类收益，属于后期资源和成长一起拉满的宗门。",
        "playbook": [
            "先看元婴状态",
            "安排出窍和问道拿资源",
            "冷却期用元婴闭关补修为并及时归窍",
        ],
        "actions": [
            _button_action(
                "status", "元婴状态", ".元婴状态", "先确认元婴当前状态和是否可出窍。"
            ),
            _button_action("trip", "元婴出窍", ".元婴出窍", "开始高阶资源日常。"),
            _button_action("ask", "问道", ".问道", "稳定拿高价值掉落。"),
            _button_action("retreat", "元婴闭关", ".元婴闭关", "冷却期间补修为。"),
            _button_action("return", "元婴归窍", ".元婴归窍", "结束出窍流程及时归位。"),
            _button_action(
                "comprehend", "参悟功法", ".参悟功法", "有余力时补功法成长。"
            ),
        ],
        "commands": [
            ".元婴状态",
            ".元婴出窍",
            ".元婴闭关",
            ".元婴归窍",
            ".问道",
            ".参悟功法",
        ],
        "notes": ["高境界再深入", "问道是稳定高价值日常", "元婴闭关有阶段结算"],
    },
]
