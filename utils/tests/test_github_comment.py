#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#

from unittest.mock import MagicMock

from github.GithubException import GithubException

from redis_benchmarks_specification.__common__.github import (
    update_comment_if_needed,
)


def test_update_comment_if_needed_handles_deleted_comment():
    """issue #252: a comment deleted (or inaccessible) between fetch and edit
    must not abort the run. update_comment_if_needed should swallow the
    GithubException and continue."""
    regression_comment = MagicMock()
    regression_comment.html_url = "https://example.com/c/1"
    regression_comment.edit.side_effect = GithubException(
        404, {"message": "Not Found"}, None
    )

    # Should NOT raise even though .edit() blows up with a 404.
    update_comment_if_needed(
        auto_approve=True,
        comment_body="new body",
        old_regression_comment_body="old body",
        regression_comment=regression_comment,
        verbose=False,
    )
    regression_comment.edit.assert_called_once()


def test_update_comment_if_needed_handles_missing_comment_object():
    """A None regression_comment (nothing to edit) must be a no-op, not an
    AttributeError."""
    update_comment_if_needed(
        auto_approve=True,
        comment_body="new body",
        old_regression_comment_body="old body",
        regression_comment=None,
        verbose=False,
    )


def test_update_comment_if_needed_noop_when_unchanged():
    """When the body is identical the function must not attempt an edit."""
    regression_comment = MagicMock()
    update_comment_if_needed(
        auto_approve=True,
        comment_body="same body",
        old_regression_comment_body="same body",
        regression_comment=regression_comment,
        verbose=False,
    )
    regression_comment.edit.assert_not_called()


def test_update_comment_if_needed_edits_on_change():
    """Happy path: a changed body triggers a single edit() call."""
    regression_comment = MagicMock()
    regression_comment.html_url = "https://example.com/c/2"
    update_comment_if_needed(
        auto_approve=True,
        comment_body="new body",
        old_regression_comment_body="old body",
        regression_comment=regression_comment,
        verbose=False,
    )
    regression_comment.edit.assert_called_once_with("new body")
