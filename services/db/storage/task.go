package storage

import (
	"context"
	"database/sql"
	"time"
)

type TaskRow struct {
	ID          string
	Type        string
	Payload     string
	Status      string // queued|running|success|failed|dead
	ScheduledAt time.Time
	StartedAt   sql.NullTime // nullable
	FinishedAt  sql.NullTime // nullable
	Attempts    int
	LastError   sql.NullString // nullable
}

const insertTask = `
INSERT INTO tasks (id, type, payload, status, scheduled_at)
VALUES ($1, $2, $3, $4, $5)
`

func CreateTask(ctx context.Context, db *sql.DB, t TaskRow) error {
	_, err := db.ExecContext(ctx, insertTask,
		t.ID,
		t.Type,
		t.Payload,
		t.Status,
		t.ScheduledAt,
	)
	return err
}

const selectTask = `
SELECT id, type, payload, status, scheduled_at, started_at, finished_at, attempts, last_error
FROM tasks
WHERE id = $1
`

func GetTask(ctx context.Context, db *sql.DB, id string) (*TaskRow, error) {
	var t TaskRow
	err := db.QueryRowContext(ctx, selectTask, id).Scan(
		&t.ID,
		&t.Type,
		&t.Payload,
		&t.Status,
		&t.ScheduledAt,
		&t.StartedAt,
		&t.FinishedAt,
		&t.Attempts,
		&t.LastError,
	)
	if err != nil {
		return nil, err
	}
	return &t, nil
}

const updateTaskStatus = `
UPDATE tasks
SET status = $2,
    attempts = $3,
    started_at = $4,
    finished_at = $5,
    last_error = $6
WHERE id = $1
`

func UpdateTaskStatus(
	ctx context.Context,
	db *sql.DB,
	id string,
	status string,
	attempts int,
	startedAt sql.NullTime,
	finishedAt sql.NullTime,
	lastError sql.NullString,
) error {
	_, err := db.ExecContext(ctx, updateTaskStatus,
		id,
		status,
		attempts,
		startedAt,
		finishedAt,
		lastError,
	)
	return err
}
