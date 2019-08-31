#include <stdio.h>
#include <mpdecimal.h>

static mpd_context_t mpd_ctx;

static mpd_t *decimal(const char *s, int digits) {
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

int main(int argc, char *argv[]) {
    mpd_ieee_context(&mpd_ctx, MPD_DECIMAL128);
    mpd_ctx.round = MPD_ROUND_DOWN;
    mpd_ctx.clamp = 1;

    printf("%ld\n", mpd_getprec(&mpd_ctx));
    printf("\n");

    mpd_t *epsilon = decimal("0.000000000000000000000000000000001", 0);
    mpd_t *delta = decimal("0.0000000000000000000000000000001", 0);

    mpd_print(epsilon);
    mpd_print(delta);
    printf("\n");

    mpd_t *dec = mpd_new(&mpd_ctx);
    mpd_copy(dec, epsilon, &mpd_ctx);

    mpd_sub(dec, dec, delta, &mpd_ctx);
    mpd_print(dec);

    mpd_add(dec, dec, delta, &mpd_ctx);
    mpd_print(dec);

    // XXX: no memory free here since just for testing purpose

    return 0;
}
