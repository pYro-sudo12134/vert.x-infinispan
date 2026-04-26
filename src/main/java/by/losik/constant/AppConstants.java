package by.losik.constant;

public class AppConstants {
    public static final int HTTP_OK = 200;
    public static final int HTTP_CREATED = 201;
    public static final int HTTP_NO_CONTENT = 204;
    public static final int HTTP_BAD_REQUEST = 400;
    public static final int HTTP_NOT_FOUND = 404;
    public static final int HTTP_CONFLICT = 409;
    public static final int HTTP_INTERNAL_ERROR = 500;

    public static final String ACTION_UPLOAD = "upload";
    public static final String ACTION_UPDATE = "update";
    public static final String ACTION_DELETE = "delete";
    public static final String ACTION_GET = "get";
    public static final String ACTION_LIST = "list";

    public static final String FIELD_ACTION = "action";
    public static final String FIELD_FILE_ID = "fileId";
    public static final String FIELD_FILE_NAME = "fileName";
    public static final String FIELD_FILE_PATH = "filePath";
    public static final String FIELD_CONTENT_TYPE = "contentType";
    public static final String FIELD_STATUS = "status";
    public static final String FIELD_FILES = "files";

    public static final String STATUS_OK = "ok";
    public static final String STATUS_UPDATED = "updated";
    public static final String STATUS_DELETED = "deleted";
    public static final String STATUS_DELETED_BUT_FILE_MAY_REMAIN = "deleted but file may remain";

    public static final String ERR_NO_FILE = "No file uploaded";
    public static final String ERR_FILE_NOT_FOUND = "File not found";
    public static final String ERR_UNKNOWN_ACTION = "Unknown action";
    public static final String ERR_UPLOAD_FAILED = "Upload failed";
    public static final String ERR_DELETE_FAILED = "Delete failed";
    public static final String ERR_UPDATE_FAILED = "Update failed";
    public static final String ERR_LIST_FAILED = "Failed to list files";
    public static final String ERR_SAVE_METADATA = "Failed to save metadata";
    public static final String ERR_COPY_TO_NFS = "Failed to copy to NFS";
    public static final String ERR_REMOVE_METADATA = "Failed to remove from metadata map";

    public static final String FIELD_PAGE = "page";
    public static final String FIELD_SIZE = "size";
    public static final String FIELD_TOTAL = "total";
    public static final String FIELD_PREFIX = "prefix";
    public static final String FIELD_SORT = "sort";
    public static final String FIELD_CREATED_AT = "createdAt";
    public static final String FIELD_DATE = "date";
    public static final String FIELD_ORDER = "order";
    public static final String FIELD_ID = "id";
    public static final String ORDER_DESC = "desc";
    public static final String FIELD_UPDATED_AT = "updatedAt";
}