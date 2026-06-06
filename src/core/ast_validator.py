import ast

def verify_expression_safety(expression: str) -> None:
    """
    Verify the safety of a factor expression or custom signal logic script.
    Checks AST for lookahead bias patterns such as:
    - shift(-N), pct_change(-N)
    - negative indices in iloc, iat, loc, at
    - positive lower limits or negative upper limits in slices (e.g., [1:] or [:-1])
    
    Raises ValueError if look-ahead bias is detected.
    """
    if not expression:
        return
    try:
        tree = ast.parse(expression)
    except SyntaxError as e:
        raise ValueError(f"Expression syntax error: {str(e)}")

    for node in ast.walk(tree):
        # 1. Block shift / pct_change negative arguments
        if isinstance(node, ast.Call):
            func_name = getattr(node.func, 'attr', None)
            if func_name in {'shift', 'pct_change'}:
                # Check positional arguments
                for arg in node.args:
                    # Negative UnaryOp (e.g. -1)
                    if isinstance(arg, ast.UnaryOp) and isinstance(arg.op, ast.USub):
                        if isinstance(arg.operand, ast.Constant) and arg.operand.value > 0:
                            raise ValueError(f"Look-ahead bias detected: negative argument in {func_name}() is forbidden.")
                        elif isinstance(arg.operand, ast.Num) and arg.operand.n > 0:
                            raise ValueError(f"Look-ahead bias detected: negative argument in {func_name}() is forbidden.")
                    # Negative constant literals directly
                    elif isinstance(arg, ast.Constant) and isinstance(val := getattr(arg, 'value', None), (int, float)) and val < 0:
                        raise ValueError(f"Look-ahead bias detected: negative argument in {func_name}() is forbidden.")

                # Check keyword arguments (e.g. periods=-5)
                for kw in node.keywords:
                    if kw.arg in {'periods', 'periods_y', 'periods_x'}:
                        val_node = kw.value
                        if isinstance(val_node, ast.UnaryOp) and isinstance(val_node.op, ast.USub):
                            if isinstance(val_node.operand, ast.Constant) and val_node.operand.value > 0:
                                raise ValueError(f"Look-ahead bias detected: negative keyword argument '{kw.arg}' in {func_name}() is forbidden.")
                            elif isinstance(val_node.operand, ast.Num) and val_node.operand.n > 0:
                                raise ValueError(f"Look-ahead bias detected: negative keyword argument '{kw.arg}' in {func_name}() is forbidden.")
                        elif isinstance(val_node, ast.Constant) and isinstance(val := getattr(val_node, 'value', None), (int, float)) and val < 0:
                            raise ValueError(f"Look-ahead bias detected: negative keyword argument '{kw.arg}' in {func_name}() is forbidden.")

        # 2. Block iloc / iat / loc / at negative subscripts (e.g. iloc[-1])
        elif isinstance(node, ast.Subscript):
            if isinstance(node.value, ast.Attribute) and node.value.attr in {'iloc', 'iat', 'loc', 'at'}:
                # Walk the subscript slice to find any negative index
                for sub_node in ast.walk(node.slice):
                    if isinstance(sub_node, ast.UnaryOp) and isinstance(sub_node.op, ast.USub):
                        if isinstance(sub_node.operand, ast.Constant) and sub_node.operand.value > 0:
                            raise ValueError(f"Look-ahead bias detected: negative indexing in {node.value.attr} is forbidden.")
                        elif isinstance(sub_node.operand, ast.Num) and sub_node.operand.n > 0:
                            raise ValueError(f"Look-ahead bias detected: negative indexing in {node.value.attr} is forbidden.")
                    elif isinstance(sub_node, ast.Constant) and isinstance(val := getattr(sub_node, 'value', None), (int, float)) and val < 0:
                        raise ValueError(f"Look-ahead bias detected: negative indexing in {node.value.attr} is forbidden.")
            
            # 3. Defense upgrade: check slicing on subscripts (e.g., [1:] or [:-1])
            elif isinstance(node.slice, ast.Slice):
                # A positive lower bound (e.g., lower=1) shifts elements backward (future leak)
                lower_node = node.slice.lower
                if lower_node is not None:
                    if isinstance(lower_node, ast.Constant) and isinstance(val := getattr(lower_node, 'value', None), (int, float)) and val > 0:
                        raise ValueError("Look-ahead bias detected: positive lower slice boundary is forbidden.")
                    elif isinstance(lower_node, ast.Num) and lower_node.n > 0:
                        raise ValueError("Look-ahead bias detected: positive lower slice boundary is forbidden.")

                # A negative upper bound (e.g., upper=-1) is also forbidden
                upper_node = node.slice.upper
                if upper_node is not None:
                    if isinstance(upper_node, ast.UnaryOp) and isinstance(upper_node.op, ast.USub):
                        if isinstance(upper_node.operand, ast.Constant) and upper_node.operand.value > 0:
                            raise ValueError("Look-ahead bias detected: negative upper slice boundary is forbidden.")
                        elif isinstance(upper_node.operand, ast.Num) and upper_node.operand.n > 0:
                            raise ValueError("Look-ahead bias detected: negative upper slice boundary is forbidden.")
                    elif isinstance(upper_node, ast.Constant) and isinstance(val := getattr(upper_node, 'value', None), (int, float)) and val < 0:
                        raise ValueError("Look-ahead bias detected: negative upper slice boundary is forbidden.")
