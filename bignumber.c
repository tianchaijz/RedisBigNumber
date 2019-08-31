/*
 * Copyright (C) Jinzheng Zhang (tianchaijz)
 */

#include "redismodule.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <mpdecimal.h>

typedef enum {
    op_add = 0,
    op_sub,
    op_mul,
    op_div,
} bn_op_t;

static mpd_context_t mpd_ctx;
static mpd_t *mpd_zero;
static mpd_t *mpd_one;

/* digits: the number of digits to appear after the decimal point. */
static inline mpd_t *decimal(const char *s, int digits) {
    mpd_ctx.status = 0;

    mpd_t *dec = mpd_new(&mpd_ctx);
    mpd_set_string(dec, s, &mpd_ctx);

    if (mpd_ctx.status == MPD_Conversion_syntax) {
        mpd_del(dec);
        return NULL;
    }

    if (digits != 0) {
        mpd_rescale(dec, dec, -digits, &mpd_ctx);
    }

    return dec;
}

static inline int bn_op_helper(RedisModuleCtx *ctx, RedisModuleString **argv,
                               int argc, bn_op_t op) {
    size_t len;
    char *str;
    const char *val;
    mpd_t *lhs, *rhs;
    RedisModuleString *dest;

    if (argc != 3) {
        return RedisModule_WrongArity(ctx);
    }

    val = RedisModule_StringPtrLen(argv[1], NULL);
    lhs = decimal(val, 0);
    if (lhs == NULL) {
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    val = RedisModule_StringPtrLen(argv[2], NULL);
    rhs = decimal(val, 0);
    if (rhs == NULL) {
        mpd_del(lhs);
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    switch (op) {
    case op_add:
        mpd_add(lhs, lhs, rhs, &mpd_ctx);
        break;
    case op_sub:
        mpd_sub(lhs, lhs, rhs, &mpd_ctx);
        break;
    case op_mul:
        mpd_mul(lhs, lhs, rhs, &mpd_ctx);
        break;
    case op_div:
        if (mpd_cmp(rhs, mpd_zero, &mpd_ctx) == 0) {
            mpd_del(lhs);
            mpd_del(rhs);
            return RedisModule_ReplyWithError(ctx, "ERR division by zero");
        }
        mpd_div(lhs, lhs, rhs, &mpd_ctx);
        break;
    }

    len = mpd_to_sci_size(&str, lhs, 0);
    dest = RedisModule_CreateString(ctx, str, len);

    free(str);
    mpd_del(lhs);
    mpd_del(rhs);

    return RedisModule_ReplyWithString(ctx, dest);
}

static inline int bn_get_helper(RedisModuleCtx *ctx, RedisModuleString *hash,
                                RedisModuleString *key, int digits) {
    static char buf[256];
    size_t len;
    char *str;
    const char *val;
    mpd_t *dec;
    RedisModuleString *dest;
    RedisModuleCallReply *reply;

    reply = hash ? RedisModule_Call(ctx, "HGET", "ss", hash, key)
                 : RedisModule_Call(ctx, "GET", "s", key);
    if (RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ERROR) {
        RedisModule_ReplyWithCallReply(ctx, reply);
        return REDISMODULE_ERR;
    }

    val = RedisModule_CallReplyStringPtr(reply, &len);
    if (val != NULL) {
        memcpy(buf, val, len);
        buf[len] = '\0';
        dec = decimal(buf, digits);
        if (dec == NULL) {
            return RedisModule_ReplyWithError(ctx,
                                              REDISMODULE_ERRORMSG_WRONGTYPE);
        }

        len = mpd_to_sci_size(&str, dec, 0);
        dest = RedisModule_CreateString(ctx, str, len);

        free(str);
        mpd_del(dec);

        return RedisModule_ReplyWithString(ctx, dest);
    }

    return RedisModule_ReplyWithCallReply(ctx, reply);
}

static inline int bn_incr_helper(RedisModuleCtx *ctx, RedisModuleString *hash,
                                 RedisModuleString *key, mpd_t *delta,
                                 int incr) {
    static char buf[256];
    size_t len;
    char *str;
    const char *val;
    mpd_t *dec;
    RedisModuleString *dest;
    RedisModuleCallReply *reply;

    reply = hash ? RedisModule_Call(ctx, "HGET", "ss", hash, key)
                 : RedisModule_Call(ctx, "GET", "s", key);
    if (RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ERROR) {
        RedisModule_ReplyWithCallReply(ctx, reply);
        return REDISMODULE_ERR;
    }

    val = RedisModule_CallReplyStringPtr(reply, &len);
    if (val != NULL) {
        memcpy(buf, val, len);
        buf[len] = '\0';
        dec = decimal(buf, 0);
        if (dec == NULL) {
            return RedisModule_ReplyWithError(ctx,
                                              REDISMODULE_ERRORMSG_WRONGTYPE);
        }
    } else {
        dec = mpd_new(&mpd_ctx);
        mpd_copy(dec, mpd_zero, &mpd_ctx);
    }

    if (incr) {
        mpd_add(dec, dec, delta, &mpd_ctx);
    } else {
        mpd_sub(dec, dec, delta, &mpd_ctx);
    }

    len = mpd_to_sci_size(&str, dec, 0);
    dest = RedisModule_CreateString(ctx, str, len);

    free(str);
    mpd_del(dec);

    reply = hash ? RedisModule_Call(ctx, "HSET", "sss!", hash, key, dest)
                 : RedisModule_Call(ctx, "SET", "ss!", key, dest);
    if (RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ERROR) {
        RedisModule_ReplyWithCallReply(ctx, reply);
        return REDISMODULE_ERR;
    }

    return RedisModule_ReplyWithString(ctx, dest);
}

static inline int bn_incrby_helper(RedisModuleCtx *ctx,
                                   RedisModuleString **argv, int argc,
                                   int incr) {
    int rc;
    const char *val;
    mpd_t *dec;
    RedisModuleString *delta;

    if (argc != 3) {
        return RedisModule_WrongArity(ctx);
    }

    delta = argv[2];
    val = RedisModule_StringPtrLen(delta, NULL);

    dec = decimal(val, 0);
    if (dec == NULL) {
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    rc = bn_incr_helper(ctx, NULL, argv[1], dec, incr);

    mpd_del(dec);

    return rc;
}

static inline int bn_hincrby_helper(RedisModuleCtx *ctx,
                                    RedisModuleString **argv, int argc,
                                    int incr) {
    int rc;
    const char *val;
    mpd_t *dec;
    RedisModuleString *delta;

    if (argc != 4) {
        return RedisModule_WrongArity(ctx);
    }

    delta = argv[3];
    val = RedisModule_StringPtrLen(delta, NULL);

    dec = decimal(val, 0);
    if (dec == NULL) {
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    rc = bn_incr_helper(ctx, argv[1], argv[2], dec, incr);

    mpd_del(dec);

    return rc;
}

int cmd_ADD(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    return bn_op_helper(ctx, argv, argc, op_add);
}

int cmd_SUB(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    return bn_op_helper(ctx, argv, argc, op_sub);
}

int cmd_MUL(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    return bn_op_helper(ctx, argv, argc, op_mul);
}

int cmd_DIV(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    return bn_op_helper(ctx, argv, argc, op_div);
}

int cmd_TO_FIXED(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    size_t len;
    char *str;
    long long digits;
    const char *val;
    mpd_t *dec;
    RedisModuleString *dest;

    RedisModule_AutoMemory(ctx);

    if (argc != 3) {
        return RedisModule_WrongArity(ctx);
    }

    if (RedisModule_StringToLongLong(argv[2], &digits) != REDISMODULE_OK) {
        return RedisModule_ReplyWithError(ctx, "ERR invalid digits parameter");
    }

    val = RedisModule_StringPtrLen(argv[1], NULL);
    dec = decimal(val, 0);
    if (dec == NULL) {
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    mpd_rescale(dec, dec, -(int)digits, &mpd_ctx);

    len = mpd_to_sci_size(&str, dec, 0);
    dest = RedisModule_CreateString(ctx, str, len);

    free(str);
    mpd_del(dec);

    return RedisModule_ReplyWithString(ctx, dest);
}

int cmd_GET(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    long long digits;

    RedisModule_AutoMemory(ctx);

    if (argc < 2 || argc > 3) {
        return RedisModule_WrongArity(ctx);
    }

    digits = 0;
    if (argc == 3) {
        if (RedisModule_StringToLongLong(argv[2], &digits) != REDISMODULE_OK) {
            return RedisModule_ReplyWithError(ctx,
                                              "ERR invalid digits parameter");
        }
    }

    return bn_get_helper(ctx, NULL, argv[1], (int)digits);
}

int cmd_INCR(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc != 2) {
        return RedisModule_WrongArity(ctx);
    }

    return bn_incr_helper(ctx, NULL, argv[1], mpd_one, 1);
}

int cmd_DECR(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc != 2) {
        return RedisModule_WrongArity(ctx);
    }

    return bn_incr_helper(ctx, NULL, argv[1], mpd_one, 0);
}

int cmd_INCRBY(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    return bn_incrby_helper(ctx, argv, argc, 1);
}

int cmd_DECRBY(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    return bn_incrby_helper(ctx, argv, argc, 0);
}

int cmd_HGET(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    long long digits;

    RedisModule_AutoMemory(ctx);

    if (argc < 3 || argc > 4) {
        return RedisModule_WrongArity(ctx);
    }

    digits = 0;
    if (argc == 4) {
        if (RedisModule_StringToLongLong(argv[3], &digits) != REDISMODULE_OK) {
            return RedisModule_ReplyWithError(ctx,
                                              "ERR invalid digits parameter");
        }
    }

    return bn_get_helper(ctx, argv[1], argv[2], (int)digits);
}

int cmd_HINCR(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc != 3) {
        return RedisModule_WrongArity(ctx);
    }

    return bn_incr_helper(ctx, argv[1], argv[2], mpd_one, 1);
}

int cmd_HDECR(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc != 3) {
        return RedisModule_WrongArity(ctx);
    }

    return bn_incr_helper(ctx, argv[1], argv[2], mpd_one, 0);
}

int cmd_HINCRBY(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    return bn_hincrby_helper(ctx, argv, argc, 1);
}

int cmd_HDECRBY(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    return bn_hincrby_helper(ctx, argv, argc, 0);
}

static inline void initMPD() {
    /* https://docs.oracle.com/javase/7/docs/api/java/math/MathContext.html.
     * DECIMAL128 is a MathContext object with a precision setting matching the
     * IEEE 754R Decimal128 format, 34 digits, and a rounding mode of
     * HALF_EVEN, the IEEE 754R default. */
    mpd_ieee_context(&mpd_ctx, MPD_DECIMAL128);
    mpd_ctx.round = MPD_ROUND_DOWN;

    mpd_zero = mpd_new(&mpd_ctx);
    mpd_set_string(mpd_zero, "0", &mpd_ctx);

    mpd_one = mpd_new(&mpd_ctx);
    mpd_set_string(mpd_one, "1", &mpd_ctx);
}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv,
                       int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    if (RedisModule_Init(ctx, "bn", 1, REDISMODULE_APIVER_1) ==
        REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "bn.add", cmd_ADD, "readonly fast", 0,
                                  0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "bn.sub", cmd_SUB, "readonly fast", 0,
                                  0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "bn.mul", cmd_MUL, "readonly fast", 0,
                                  0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "bn.div", cmd_DIV, "readonly fast", 0,
                                  0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "bn.to_fixed", cmd_TO_FIXED,
                                  "readonly fast", 0, 0,
                                  0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "bn.get", cmd_GET, "readonly", 1, 1,
                                  1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "bn.incr", cmd_INCR, "write deny-oom",
                                  1, 1, 1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "bn.decr", cmd_DECR, "write deny-oom",
                                  1, 1, 1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "bn.incrby", cmd_INCRBY,
                                  "write deny-oom", 1, 1,
                                  1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "bn.decrby", cmd_DECRBY,
                                  "write deny-oom", 1, 1,
                                  1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "bn.hget", cmd_HGET, "readonly", 1, 1,
                                  1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "bn.hincr", cmd_HINCR, "write deny-oom",
                                  1, 1, 1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "bn.hdecr", cmd_HDECR, "write deny-oom",
                                  1, 1, 1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "bn.hincrby", cmd_HINCRBY,
                                  "write deny-oom", 1, 1,
                                  1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "bn.hdecrby", cmd_HDECRBY,
                                  "write deny-oom", 1, 1,
                                  1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    initMPD();

    return REDISMODULE_OK;
}
