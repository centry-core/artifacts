const ArtifactFilesTable = {
    props: ['selected-bucket', ],
    data() {
        return {
            taskResults: {},
        }
    },
    methods: {
        uploadFiles(e) {
            const files = e.dataTransfer.files;
            files.forEach((file) => {
                this._uploadFile(file);
            })
        },
        _uploadFile(file) {
            const formData = new FormData()
            formData.append('file', file)
            $.ajax({
                url: `/api/v1/artifacts/artifacts/${getSelectedProjectId()}/${this.selectedBucket.name}`,
                type: 'POST',
                data: formData,
                contentType: false,
                processData: false,
                success: (res) => {
                    this.$emit('refresh', res.size);
                    vueVm.registered_components.storage.getData();
                }
            });
        },
        deleteFiles() {
            let url = `/api/v1/artifacts/artifacts/${getSelectedProjectId()}/${this.selectedBucket.name}?`
            if ($("#artifact-table").bootstrapTable('getSelections').length > 0) {
                $("#artifact-table").bootstrapTable('getSelections').forEach(item => {
                    url += "fname[]=" + item["name"] + "&"
                });
                $.ajax({
                    url: url.substring(0, url.length - 1),
                    type: 'DELETE',
                    success: (res) => {
                        this.$emit('refresh', res.size);
                        vueVm.registered_components.storage.getData();
                    }
                });
            }
        },
        deleteFile(fileName, index) {
            $.ajax({
                url: `/api/v1/artifacts/artifact/${getSelectedProjectId()}/${this.selectedBucket.name}/${fileName}`,
                type: 'DELETE',
                success: (res) => {
                    $('#artifact-table').bootstrapTable('remove', {
                        field: '$index',
                        values: [index]
                    })
                    this.$emit('refresh', res.size);
                    showNotify('SUCCESS', 'File delete.');
                }
            });
        },
    },
    template: `
        <div class="card mt-3 mr-3 card-table-sm w-100" @dragover.prevent @drop.prevent>
            <div class="row p-3">
                <div class="col-4">
                    <h4>Bucket {{ selectedBucket.name }}</h4>   
                    <p class="font-h6 font-weight-400">Retention policy - <span id="filesRetentionPolicy"></span></p>
                </div>
                <div class="col-8">
                    <div class="d-flex justify-content-end">
                        <button type="button" 
                            @click="deleteFiles"
                            class="btn btn-secondary btn-sm btn-icon__sm mr-2">
                            <i class="fas fa-trash-alt"></i>
                        </button>
                    </div>
                </div>
            </div>
            <div class="card-body" @drop="uploadFiles">
                <table class="table table-borderless"
                    id="artifact-table"
                    data-toggle="table"
                    data-unique-id="id"
                    data-page-list="[5, 10, 15]"
                    data-pagination="true"
                    data-pagination-pre-text="<img src='/design-system/static/assets/ico/arrow_left.svg'>"
                    data-pagination-next-text="<img src='/design-system/static/assets/ico/arrow_right.svg'>"
                    data-page-size=5>
                    <thead class="thead-light">
                        <tr>
                            <th scope="col" data-checkbox="true"></th>
                            <th scope="col" data-sortable="true" data-field="name" class="w-100">NAME</th>
                            <th scope="col" data-sortable="true" data-cell-style="nameStyle" data-field="size" data-sorter="filesizeSorter">SIZE</th>
                            <th scope="col" data-sortable="true" data-cell-style="styleNoWrapText" data-field="modified">LAST UPDATE</th>
                            <th scope="col" data-field="actions" data-align="right" 
                                data-formatter="filesFormatter.actions" 
                                data-events="filesFormatter.events"></th>
                        </tr>
                    </thead>
                    <tbody>
                    </tbody>
                </table>
            </div>
        </div>
    `
}

register_component('artifact-files-table', ArtifactFilesTable);