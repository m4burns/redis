#lang racket

; redis bindings for racket

; based on protocol described here: http://redis.io/topics/protocol

;; TODO:
;; [x] 2013-09-25: make redis-specific exn
;;     DONE: 2013-09-26
;; [x] 2013-09-25: dont automatically set rconn parameter on connect
;;                 - makes it hard to use parameterize with (connect)
;;     DONE: 2013-09-26

(require (for-syntax racket/base syntax/parse)
         racket/tcp racket/match 
         racket/async-channel racket/contract racket/port
         "redis-error.rkt" "bytes-utils.rkt" "constants.rkt"
         "nodelay.rkt" (rename-in ffi/unsafe [-> ->/f]))
(provide connect disconnect (struct-out redis-connection)
         send-cmd send-cmd/no-reply get-reply get-reply/port
         get-reply-evt get-reply-evt/port pubsub-connection?
         pubsub-connect make-subscribe-chan current-redis-connection)

;; connect/disconnect ---------------------------------------------------------
;; owner = thread or #f; #f = pool thread owns it
(struct redis-connection (in out owner [pubsub #:auto #:mutable] [closed? #:auto #:mutable]))

(define current-redis-connection (make-parameter #f))

(define/contract (connect [host LOCALHOST] [port DEFAULT-REDIS-PORT])
  (->* () (string? (integer-in 1 65535)) redis-connection?)
  (define-values (in out) (tcp-connect host port))
  (set-tcp-nodelay! out #t)
  (file-stream-buffer-mode out 'block)
  (define conn (redis-connection in out (current-thread)))
  (register-finalizer conn disconnect)
  conn)

(define/contract (disconnect conn)
  (-> redis-connection? void?)
  (match-define (redis-connection in out _ _ closed?) conn)
  (void
    (unless closed?
      (dynamic-wind
        void
        (lambda _ (send-cmd/no-reply #:rconn conn "QUIT"))
        (lambda _
          (close-input-port in)
          (close-output-port out)
          (set-redis-connection-closed?! conn #t))))))

;; pubsub-specific connections ------------------------------------------------
;; A pubsub connection spins up a thread to handle subscribe/unsubscribe msgs
;;  and distribute other messages to the appropriate channels
;; This thread is managed by a dedicated custodian

(define (pubsub-connection? conn) (and (redis-connection? conn) (redis-connection-pubsub conn) #t))

(define (pubsub-connect [host LOCALHOST] [port DEFAULT-REDIS-PORT])
  (define conn (connect host port))
  (define subscribers (make-hash)) ;; key -> list of subscribed channels
  (define pubsub-th ; thread to check for sub/unsub confirmations and distribute messages
    (thread (λ ()
      (define in (redis-connection-in conn))
      (let loop ()
        (define reply
          (with-handlers
            ([exn? (lambda _ eof)])
            (get-reply/port in)))
        (unless (eof-object? reply)
          (cond
            [(bytes=? (car reply) #"subscribe") (void)]
            [(bytes=? (car reply) #"psubscribe") (void)]
            [(bytes=? (car reply) #"unsubscribe") (void)]
            [(bytes=? (car reply) #"punsubscribe") (void)]
            ;; subscribe message
            [(bytes=? (car reply) #"message")
             (let ([chans (hash-ref subscribers (cadr reply) #f)])
               (when chans
                 (map (λ (c) (async-channel-put c (caddr reply))) chans)))]
            ;; psubscribe message
            [(bytes=? (car reply) #"pmessage")
             (let ([chans (hash-ref subscribers (cadr reply) #f)])
               (when chans
                 (map (λ (c) (async-channel-put c (cddr reply))) chans)))])
          (loop))))))
  (set-redis-connection-pubsub! conn subscribers)
  conn)

;; make-subscribe-chan : 
;; - (p)subscribes to a key on a pubsub connection
;; - returns an async-channel that listens for that key's messages
(define (make-subscribe-chan conn key
                             #:psubscribe [psub? #f])
  (match-define (redis-connection in _ _ pubsub _) conn)
  (unless pubsub (redis-error "Tried to SUBSCRIBE on non pubsub connection."))
  (send-cmd/no-reply #:rconn conn (if psub? 'psubscribe 'subscribe) key)
  (define ch (make-async-channel))
  (define bytes-key (or (and (string? key) (string->bytes/utf-8 key))
                        (and (symbol? key) (string->bytes/utf-8 (symbol->string key)))
                        key))
  (hash-update! pubsub bytes-key (λ (v) (cons ch v)) null)
  ch)

;; send cmd/recv reply --------------------------------------------------------

;; send-cmd/no-reply : sends specified cmd; does not wait for reply
;; - Use conn if provided.
;; - If no conn, then connect and set current-redis-connection parameter
(define (send-cmd/no-reply #:rconn [conn (current-redis-connection)]
                           #:host [host LOCALHOST]
                           #:port [port DEFAULT-REDIS-PORT] 
                           cmd . args)
  (define rconn 
    (or (and (redis-connection? conn)
             (eq? (current-thread) (redis-connection-owner conn))
             conn)
        (let ([new-conn (connect host port)])
          (current-redis-connection new-conn)
          new-conn)))
  (match-define (redis-connection in out _ _ _) rconn)
  (write-bytes (mk-request cmd args) out)
  (flush-output out)
  rconn)

;; send-cmd : sends given cmd and waits for reply
(define (send-cmd #:rconn [conn (current-redis-connection)]
                  #:host [host LOCALHOST]
                  #:port [port DEFAULT-REDIS-PORT] 
                  cmd . args)
  (define rconn 
    (apply send-cmd/no-reply #:rconn conn #:host host #:port port cmd args))
  ;; must catch and re-throw here to display offending cmd and args
  (with-handlers ([exn:fail?
                   (lambda (x)
                     (redis-error (format "~a\nCMD: ~a\nARGS: ~a\n"
                                          (exn-message x) cmd args)))])
    (get-reply rconn)))

(define (mk-request cmd args)
  (bytes-append
   (string->bytes/utf-8
    (string-append "*" (number->string (add1 (length args))) "\r\n"))
   (arg->bytes cmd)
   (apply bytes-append (map arg->bytes args))))

(define (arg->bytes val)
  (define bs
    (cond [(bytes? val) val]
          [(string? val) (string->bytes/utf-8 val)]
          [(number? val) (number->bytes val)]
          [(symbol? val) (symbol->bytes val)]
          [else (error 'send "invalid argument: ~v\n" val)]))
  (bytes-append #"$" (number->bytes (bytes-length bs)) CRLF bs CRLF))

;; get-reply : reads the reply from a redis-connection
;; cmd and args are used for error reporting
;; default in port is the connection for the current thread
(define (get-reply [conn (current-redis-connection)])
  (get-reply/port (redis-connection-in conn)))
(define (get-reply/port [in (redis-connection-in (current-redis-connection))])
  (define byte1 (read-char in))
  (define reply1 (read-line in 'return-linefeed))
  (match byte1
    [#\+ reply1] ; Status reply
    [#\- (error 'redis-reply reply1)] ; Error reply
    [#\: (string->number reply1)]     ; Integer reply
    [#\$ (let ([numbytes (string->number reply1)])   ; Bulk reply
           (if (= numbytes -1) #\null
               (begin0
                 (read-bytes numbytes in)
                 (read-line in 'return-linefeed))))]
    [#\* (let ([numreplies (string->number reply1)]) ; Multi-bulk reply
           (if (= numreplies -1) #\null
               (for/list ([n (in-range numreplies)]) (get-reply/port in))))]))

(define (get-reply-evt [conn (current-redis-connection)])
  (get-reply-evt/port (redis-connection-in conn)))
  
(define (get-reply-evt/port 
            [in (redis-connection-in (current-redis-connection))])
  (wrap-evt in get-reply/port))

