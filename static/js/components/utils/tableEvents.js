var bucketEvents = {
    "click .bucket_delete": function (e, value, row, index) {
        e.stopPropagation();
        const vm = vueVm.registered_components.artifact
        vm.openConfirm('single');
    },
    "click .bucket_setting": function (e, value, row, index) {
        e.stopPropagation();
        $('#bucketUpdateModal').modal('show');
    }
}