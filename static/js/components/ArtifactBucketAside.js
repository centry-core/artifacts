const ArtifactBucketAside = {
    props: ['isInitDataFetched', 'selectedBucketRowIndex'],
    data() {
        return {
            canSelectItems: false,
            loadingDelete: false,
            checkedBucketsList: [],
        }
    },
    computed: {
        isAnyBucketSelected() {
            return this.checkedBucketsList.length > 0;
        }
    },
    watch: {
        isInitDataFetched() {
            this.setBucketEvents();
        }
    },
    methods: {
        setBucketEvents() {
            $('#bucket-table').on('check.bs.table', (row, $element) => {
                this.checkedBucketsList.push($element.name);
            });
            $('#bucket-table').on('uncheck.bs.table', (row, $element) => {
                this.checkedBucketsList = this.checkedBucketsList.filter(bucket => {
                    return $element.name !== bucket
                })
            });
            $('#bucket-table').on('uncheck-all.bs.table', (row, $element) => {
                this.checkedBucketsList = [];
            });
            $('#bucket-table').on('check-all.bs.table', (rowsAfter, rowsBefore) => {
                this.checkedBucketsList = rowsBefore.map(row => row.name);
            });
        },
        switchSelectItems() {
            this.canSelectItems = !this.canSelectItems;
            const action = this.canSelectItems ? 'hideColumn' : 'showColumn';
            $('#bucket-table').bootstrapTable(action, 'select');
            document.getElementById("bucket-table")
                .rows[this.selectedBucketRowIndex + 1]
                .classList.add('highlight');
        },
    },
    template: `
        <aside class="m-3 card position-sticky" style="width: 400px">
            <div class="row p-3">
                <div class="col-4">
                    <h4>Bucket</h4>
                </div>
                <div class="col-8">
                    <div class="d-flex justify-content-end">
                        <button type="button"
                             data-toggle="modal" 
                             data-target="#bucketModal"
                             class="btn btn-secondary btn-sm btn-icon__sm mr-2">
                            <i class="fas fa-plus"></i>
                        </button>
                        <button type="button" class="btn btn-secondary btn-sm btn-icon__sm mr-2"
                            @click="switchSelectItems">
                            <i class="icon__18x18 icon-multichoice"></i>
                        </button>
                        <button type="button" 
                            @click="$emit('open-confirm', 'multiple')"
                            :disabled="!isAnyBucketSelected"
                            class="btn btn-secondary btn-sm btn-icon__sm mr-2">
                            <i class="fas fa-trash-alt"></i>
                        </button>
                    </div>
                </div>
            </div>
            <div>
                <table class="table table-borderless"
                    id="bucket-table"
                    data-toggle="table">
                    <thead class="thead-light">
                        <tr>
                            <th data-checkbox="true" data-field="select"></th>
                            <th scope="col" data-sortable="true" data-field="name" class="w-100 bucket-name">NAME</th>
                            <th scope="col" data-sortable="true" data-cell-style="nameStyle" data-field="size">SIZE</th>
                            <th scope="col" data-cell-style="nameStyle" 
                                data-formatter='<div class="d-none">
                                    <button class="btn btn-default btn-xs btn-table btn-icon__xs bucket_delete"><i class="fas fa-trash-alt"></i></button>
                                    <button class="btn btn-default btn-xs btn-table btn-icon__xs bucket_setting"><i class="fas fa-gear"></i></button>
                                </div>'
                                data-events="bucketEvents">
                            </th>
                        </tr>
                    </thead>
                    <tbody>
                    </tbody>
                </table>
            </div>
        </aside>
    `
}

var bucketEvents = {
    "click .bucket_delete": function (e, value, row, index) {
        e.stopPropagation();
        const vm = vueVm.registered_components.artifact
        vm.openConfirm('single');
    },
    "click .bucket_setting": function (e, value, row, index) {
        console.log("bucket_setting", row, index)
    }
}