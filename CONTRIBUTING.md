# 参与贡献

感谢你考虑为 flash-store 提交改动。项目目前处于实验性 v0 阶段，维护重点是明确行为边界、保证数据格式兼容，并为已有能力补充可重复验证。

## 开发环境

需要：

- Go 1.25 或更高版本
- GNU Make 或兼容实现
- 修改 `.proto` 文件时需要 protoc 36.2 和 `protoc-gen-go` v1.36.12

克隆仓库后运行：

```bash
make check
make test-race
```

`make check` 会检查 Go 格式、模块依赖、构建、静态分析和单元测试。

## 修改 protobuf

生成文件和 `.proto` 源文件一起提交：

```bash
make generate
make generate-check
```

如果生成结果与预期不一致，请先确认工具版本，不要手工编辑 `*.pb.go`。

## 提交改动

- 每个 Pull Request 只解决一个清晰的问题，避免顺带重构。
- 行为变更需要对应测试，并同步 README 或 `docs/` 中的相关说明。
- WAL、snapshot、schema 演进和并发行为属于兼容性敏感区域，请在描述中说明旧数据和升级路径的影响。
- 不要提交凭据、生产数据、客户信息或内部基础设施地址。
- 提交前确保 `make check` 和 `make test-race` 通过。

## API 与兼容性

项目尚未发布稳定版本，因此维护者可能在 `v0` 阶段调整公共 API 和持久化格式。即便如此，任何兼容性变化都应在 Pull Request 中明确说明，并尽可能提供迁移方案。
