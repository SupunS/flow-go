// Code generated by "stringer -type=ExpressionKind"; DO NOT EDIT.

package common

import "strconv"

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[ExpressionKindUnknown-0]
	_ = x[ExpressionKindCreate-1]
	_ = x[ExpressionKindDestroy-2]
}

const _ExpressionKind_name = "ExpressionKindUnknownExpressionKindCreateExpressionKindDestroy"

var _ExpressionKind_index = [...]uint8{0, 21, 41, 62}

func (i ExpressionKind) String() string {
	if i < 0 || i >= ExpressionKind(len(_ExpressionKind_index)-1) {
		return "ExpressionKind(" + strconv.FormatInt(int64(i), 10) + ")"
	}
	return _ExpressionKind_name[_ExpressionKind_index[i]:_ExpressionKind_index[i+1]]
}
