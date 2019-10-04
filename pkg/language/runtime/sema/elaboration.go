package sema

import "github.com/dapperlabs/flow-go/pkg/language/runtime/ast"

type Elaboration struct {
	FunctionDeclarationFunctionTypes   map[*ast.FunctionDeclaration]*FunctionType
	VariableDeclarationValueTypes      map[*ast.VariableDeclaration]Type
	VariableDeclarationTargetTypes     map[*ast.VariableDeclaration]Type
	AssignmentStatementValueTypes      map[*ast.AssignmentStatement]Type
	AssignmentStatementTargetTypes     map[*ast.AssignmentStatement]Type
	CompositeDeclarationTypes          map[*ast.CompositeDeclaration]*CompositeType
	InitializerFunctionTypes           map[*ast.InitializerDeclaration]*ConstructorFunctionType
	FunctionExpressionFunctionType     map[*ast.FunctionExpression]*FunctionType
	InvocationExpressionArgumentTypes  map[*ast.InvocationExpression][]Type
	InvocationExpressionParameterTypes map[*ast.InvocationExpression][]Type
	InterfaceDeclarationTypes          map[*ast.InterfaceDeclaration]*InterfaceType
	FailableDowncastingTypes           map[*ast.FailableDowncastExpression]Type
	ReturnStatementValueTypes          map[*ast.ReturnStatement]Type
	ReturnStatementReturnTypes         map[*ast.ReturnStatement]Type
	BinaryExpressionResultTypes        map[*ast.BinaryExpression]Type
	BinaryExpressionRightTypes         map[*ast.BinaryExpression]Type
	MemberExpressionMembers            map[*ast.MemberExpression]*Member
}

func NewElaboration() *Elaboration {
	return &Elaboration{
		FunctionDeclarationFunctionTypes:   map[*ast.FunctionDeclaration]*FunctionType{},
		VariableDeclarationValueTypes:      map[*ast.VariableDeclaration]Type{},
		VariableDeclarationTargetTypes:     map[*ast.VariableDeclaration]Type{},
		AssignmentStatementValueTypes:      map[*ast.AssignmentStatement]Type{},
		AssignmentStatementTargetTypes:     map[*ast.AssignmentStatement]Type{},
		CompositeDeclarationTypes:          map[*ast.CompositeDeclaration]*CompositeType{},
		InitializerFunctionTypes:           map[*ast.InitializerDeclaration]*ConstructorFunctionType{},
		FunctionExpressionFunctionType:     map[*ast.FunctionExpression]*FunctionType{},
		InvocationExpressionArgumentTypes:  map[*ast.InvocationExpression][]Type{},
		InvocationExpressionParameterTypes: map[*ast.InvocationExpression][]Type{},
		InterfaceDeclarationTypes:          map[*ast.InterfaceDeclaration]*InterfaceType{},
		FailableDowncastingTypes:           map[*ast.FailableDowncastExpression]Type{},
		ReturnStatementValueTypes:          map[*ast.ReturnStatement]Type{},
		ReturnStatementReturnTypes:         map[*ast.ReturnStatement]Type{},
		BinaryExpressionResultTypes:        map[*ast.BinaryExpression]Type{},
		BinaryExpressionRightTypes:         map[*ast.BinaryExpression]Type{},
		MemberExpressionMembers:            map[*ast.MemberExpression]*Member{},
	}
}
