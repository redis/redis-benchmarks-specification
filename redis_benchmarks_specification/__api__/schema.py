from marshmallow import Schema, fields


class CommitSchema(Schema):
    git_branch = fields.String(required=False)
    git_tag = fields.String(required=False)
    git_hash = fields.String(required=True)
