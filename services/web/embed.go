package web

import "embed"

//go:embed index.html
//go:embed static/*
var FS embed.FS
