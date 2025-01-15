def transform_fibonacci(n):
    if n < 0:
        return ([], [])

    fib_num_origin = [0, 1]

    for i in range(2, n):
        fib_num_origin.append(fib_num_origin[-1] + fib_num_origin[-2])

    fib_num_transform = []
    for num in fib_num_origin[:n]:
        if num % 2 == 0:
            fib_num_transform.append(num - 1)
        else:
            fib_num_transform.append(num + 1)

    return (fib_num_origin, fib_num_transform)

# Пример использования
n = 10
original, transformed = transform_fibonacci(n)
print(f"Оригинальный ряд Фибоначчи до {n}-го числа: {original}")
print(f"Преобразованный ряд Фибоначчи до {n}-го числа: {transformed}")