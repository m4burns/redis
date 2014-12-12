#lang racket
(require "redis.rkt" "redis-cmds.rkt" "redis-error.rkt")
(require rackunit)
(provide (all-defined-out))

(define-syntax-rule (test tst ...)
  (test-begin
    (parameterize ([current-redis-connection (connect)])
      (let* ([keys (KEYS "*")]
             [old (map (lambda (x) (DUMP x)) keys)])
        (dynamic-wind
          (lambda _ (for-each (lambda (x) (check-equal? (DEL x) 1)) keys))
          (lambda _ tst ...)
          (lambda _
            (disconnect (current-redis-connection))
            (current-redis-connection (connect))
            (for-each (lambda (k v) (DEL k) (RESTORE k 0 v)) keys old)))))))

(define-syntax-rule (check-redis-exn e)
  (check-exn exn:fail:redis? (lambda () e)))

(define-syntax-rule (check-set-equal? e (x ...))
  (check-equal? (list->set e) (set x ...)))

(define-syntax-rule (check-void? e) (check-equal? e (void)))
(define-syntax-rule (check-redis-connection? e) 
  (check-true (redis-connect? e)))
