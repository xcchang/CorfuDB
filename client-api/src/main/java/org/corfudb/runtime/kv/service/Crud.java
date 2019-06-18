package org.corfudb.runtime.kv.service;

public interface Crud {

    enum CrudType {
        CREATE, GET, UPDATE, DELETE
    }

    @FunctionalInterface
    interface CrudOperation<Result> {
        Result apply();
    }

    @FunctionalInterface
    interface QueryOperation<Result> extends CrudOperation<Result> {
        default CrudType type() {
            return CrudType.GET;
        }
    }

    @FunctionalInterface
    interface CreateOperation<Result> extends CrudOperation<Result> {
        default CrudType type() {
            return CrudType.CREATE;
        }
    }

    @FunctionalInterface
    interface UpdateOperation<Result> extends CrudOperation<Result> {
        default CrudType type() {
            return CrudType.UPDATE;
        }
    }

    @FunctionalInterface
    interface DeleteOperation<Result> extends CrudOperation<Result> {
        default CrudType type() {
            return CrudType.DELETE;
        }
    }
}
