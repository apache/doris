(module
    (func $add (param f32) (param f32) (result f32)
	(local.get 0)
	(local.get 1)
	(f32.add)
    )
  (export "add" (func $add))
)
