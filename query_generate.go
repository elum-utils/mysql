package mysql

import "sync"

var keyBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 1024) // начальный размер
		return &b
	},
}

// generateQuery делает минимальные аллокации: строит в байтовом срезе и конвертирует в string.
func generateQuery(params Params) string {
	if params.Query != "" {
		return params.Query
	}

	argCount := len(params.Args)
	procLen := len(params.Exec)
	dbLen := len(params.Database)

	// Быстрая оценка длины
	size := 6 + procLen + 2 // "CALL " + name + "()"
	if dbLen > 0 {
		size += dbLen + 1
	}
	if argCount > 0 {
		size += argCount*2 - 1
	}

	// Получаем временный буфер из пула
	p := keyBufPool.Get().(*[]byte)
	buf := *p
	if cap(buf) < size {
		buf = make([]byte, 0, size)
	} else {
		buf = buf[:0]
	}

	buf = append(buf, "CALL "...)
	if dbLen > 0 {
		buf = append(buf, params.Database...)
		buf = append(buf, '.')
	}
	buf = append(buf, params.Exec...)
	buf = append(buf, '(')
	if argCount > 0 {
		for i := 0; i < argCount; i++ {
			if i > 0 {
				buf = append(buf, ',', ' ')
			}
			buf = append(buf, '?')
		}
	}
	buf = append(buf, ')')

	// Создаём итоговую строку (копируется)
	result := string(buf)

	// Возвращаем буфер в пул
	*p = buf[:0]
	keyBufPool.Put(p)

	return result
}
