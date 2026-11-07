package ru.yandex.practicum.inventory;

import ru.yandex.practicum.inventory.dto.UpdateInventoryRequest;

final class Stubs {
    static final long LAST_ITEM_PRODUCT_ID = 300001L;
    static final long UPDATE_AND_RESERVE_PRODUCT_ID = 300002L;
    static final long DUPLICATE_PRODUCT_ID = 300003L;
    static final int SINGLE_ITEM = 1;
    static final int EMPTY_STOCK = 0;

    private Stubs() {
    }

    static UpdateInventoryRequest inventory(long productId, int quantity) {
        return new UpdateInventoryRequest(productId, quantity);
    }
}
