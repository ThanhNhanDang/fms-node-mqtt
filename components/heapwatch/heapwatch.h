/* heapwatch: canh chung day heap — CONG CU CHAN DOAN TAM THOI.
 *
 * Ly do ton tai: 19/09 node co mot cu nho ~21 KB xay ra MOT lan trong
 * khoang phut 1-9 roi nha ra (min_heap 9 536 -> 3 096 B) ma khong dong
 * nhat ky nao quanh do. Doan mo thi khong ra: run_print_jobs bi chan boi
 * co khong bao gio bat, check_config_update dang tat.
 *
 * Cach dung: rai heap_mark("ten-cho") o cac diem nghi. Ham chi in khi
 * cham DAY MOI, nen nhat ky khong bi ngap — no tu chi ra cho nao an bo nho.
 *
 * GO DI sau khi tim ra nguyen nhan. Day khong phai ma san xuat.
 */
#ifndef FMS_HEAPWATCH_H
#define FMS_HEAPWATCH_H

#ifdef __cplusplus
extern "C" {
#endif

void heap_mark(const char *where);

#ifdef __cplusplus
}
#endif

#endif /* FMS_HEAPWATCH_H */
