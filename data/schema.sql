[
    {
        "name": "MesMachineMaintain",
        "comment": "设备维修记录",
        "columns": [
            {
                "name": "Id",
                "type": "bigint",
                "length": 19,
                "scale": 0,
                "nullable": false,
                "comment": "主键Id"
            },
            {
                "name": "Maintain_code",
                "type": "nvarchar",
                "length": 200,
                "scale": null,
                "nullable": true,
                "comment": "维修单号"
            },
            {
                "name": "MachineId",
                "type": "bigint",
                "length": 19,
                "scale": 0,
                "nullable": true,
                "comment": "设备Id"
            },
            {
                "name": "Machine_code",
                "type": "nvarchar",
                "length": 200,
                "scale": null,
                "nullable": true,
                "comment": "设备编码"
            },
            {
                "name": "Machine_name",
                "type": "nvarchar",
                "length": 200,
                "scale": null,
                "nullable": true,
                "comment": "设备名称"
            },
            {
                "name": "RepairTime",
                "type": "datetime",
                "length": null,
                "scale": null,
                "nullable": true,
                "comment": "报修时间"
            },
            {
                "name": "RepairUserId",
                "type": "nvarchar",
                "length": 200,
                "scale": null,
                "nullable": true,
                "comment": "报修人员"
            },
            {
                "name": "RepairImg",
                "type": "nvarchar",
                "length": 500,
                "scale": null,
                "nullable": true,
                "comment": "维修图片"
            },
            {
                "name": "FaultDescription",
                "type": "nvarchar",
                "length": 1000,
                "scale": null,
                "nullable": true,
                "comment": "故障描述"
            },
            {
                "name": "MaintainState",
                "type": "int",
                "length": 10,
                "scale": 0,
                "nullable": true,
                "comment": "维修状态"
            },
            {
                "name": "MaintainContent",
                "type": "nvarchar",
                "length": 1000,
                "scale": null,
                "nullable": true,
                "comment": "维保内容"
            },
            {
                "name": "MaintainUserId",
                "type": "nvarchar",
                "length": 200,
                "scale": null,
                "nullable": true,
                "comment": "维保人"
            },
            {
                "name": "MaintainEndTime",
                "type": "datetime",
                "length": null,
                "scale": null,
                "nullable": true,
                "comment": "维修结束时间"
            },
            {
                "name": "MaintenanceTime",
                "type": "nvarchar",
                "length": 200,
                "scale": null,
                "nullable": true,
                "comment": "维保用时"
            },
            {
                "name": "CreateTime",
                "type": "datetime",
                "length": null,
                "scale": null,
                "nullable": true,
                "comment": "创建时间"
            },
            {
                "name": "UpdateTime",
                "type": "datetime",
                "length": null,
                "scale": null,
                "nullable": true,
                "comment": "更新时间"
            },
            {
                "name": "CreateUserId",
                "type": "bigint",
                "length": 19,
                "scale": 0,
                "nullable": true,
                "comment": "创建者Id"
            },
            {
                "name": "CreateUserName",
                "type": "nvarchar",
                "length": 64,
                "scale": null,
                "nullable": true,
                "comment": "创建者姓名"
            },
            {
                "name": "UpdateUserId",
                "type": "bigint",
                "length": 19,
                "scale": 0,
                "nullable": true,
                "comment": "修改者Id"
            },
            {
                "name": "UpdateUserName",
                "type": "nvarchar",
                "length": 64,
                "scale": null,
                "nullable": true,
                "comment": "修改者姓名"
            },
            {
                "name": "CreateOrgId",
                "type": "bigint",
                "length": 19,
                "scale": 0,
                "nullable": true,
                "comment": "创建者部门Id"
            },
            {
                "name": "CreateOrgName",
                "type": "nvarchar",
                "length": 64,
                "scale": null,
                "nullable": true,
                "comment": "创建者部门名称"
            },
            {
                "name": "IsDelete",
                "type": "bit",
                "length": null,
                "scale": null,
                "nullable": false,
                "comment": "软删除"
            }
        ],
        "foreign_keys": [],
        "example_data": null
    }
]