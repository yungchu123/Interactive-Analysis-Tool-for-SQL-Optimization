import json
import tkinter as tk
from tkinter import ttk

import customtkinter

import psycopg2
from psycopg2 import extras

customtkinter.set_appearance_mode('light')
customtkinter.set_default_color_theme('green')

MAIN_COLOR = "#212121"
SEC_COLOR = "#373737"
THIRD_COLOR = "#A4A4A4"
FOURTH_COLOR = "#828282"
FONT_COLOR = "#242424"

class MainApplication(tk.Tk):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.title("Query Explainer")
        self.geometry("1200x800")

        # Main container that will hold the frames
        self.container = ttk.Frame(self)
        self.container.pack(side="top", fill="both", expand=True)

        # Dictionary to store the frames
        self.frames = {}

        # Add frames to the dictionary
        for F in (ConnectionPage, QueryPage):
            frame = F(self.container, self)
            self.frames[F] = frame
            frame.grid(row=0, column=0, sticky="nsew")

        self.show_frame(ConnectionPage)

    def show_frame(self, frame_class):
        if frame_class in self.frames:
        # If the frame exists, destroy it before recreating it
            self.frames[frame_class].destroy()

        # Create a new frame and add it to the dictionary
        frame = frame_class(self.container, self)
        self.frames[frame_class] = frame

        # Position the frame in the container and raise it to the top
        frame.grid(row=0, column=0, sticky="nsew")
        frame.tkraise()

class ConnectionPage(ttk.Frame):

    def __init__(self, parent, controller):
        super().__init__(parent)
        self.entry_widgets = []  # Add this line to store entry widgets

        # create the main frame
        self.main_frame = tk.Frame(self, width=1200, height=800, bg=MAIN_COLOR)
        self.main_frame.pack(side="right", fill="both", expand=True)
        self.main_frame.pack_propagate(0)

        # create the inner container
        self.inner_frame = tk.Frame(self.main_frame, width=600, height=800, bg=MAIN_COLOR)
        self.inner_frame.pack(anchor="center")
        self.inner_frame.pack_propagate(0)

        # add a label to the inner container
        input_label = tk.Label(self.inner_frame, text="Query Juicer", font=("Arial", 54, "bold"),
                               bg=MAIN_COLOR, fg="white")
        input_label.pack(pady=(30, 70))

        # create the inner inner container
        self.inner_inner_frame = tk.Frame(self.inner_frame, width=600, bg=MAIN_COLOR,
                                          highlightbackground=SEC_COLOR, highlightthickness=2)
        self.inner_inner_frame.pack(anchor="center")

        # add a label to the right container
        input_label = tk.Label(self.inner_inner_frame, text="Enter the Connection Details:", font=("Arial", 28, "bold"),
                               bg=MAIN_COLOR, fg="white")
        input_label.pack(pady=(10, 30))

        # create the input rows
        rows = []
        label_names = ['Host IP', "Port Number", "Database Name", 'Username', "Password"]
        for i in range(5):
            # creating a frame for each row of label and input
            row = tk.Frame(self.inner_inner_frame, bg=MAIN_COLOR)
            row.pack(fill="x", padx=20, pady=5)

            # creating a label
            label = tk.Label(row, text=f"{label_names[i] + ':'}", font=("Arial", 28, "bold"), bg=MAIN_COLOR, fg="white",
                             width=15, anchor="w")
            label.pack(side="left")

            # creating an input entry
            entry = customtkinter.CTkEntry(master=row,
                                           width=300,
                                           height=45,
                                           fg_color=SEC_COLOR,
                                           text_color="white",
                                           font=("Arial", 28, "normal"),
                                           border_width=2,
                                           corner_radius=10)
            entry.pack(anchor=tk.E, side="right", fill="x", expand=True)
            self.entry_widgets.append(entry)  # Store the entry widget

            # appending the label and entry to the rows list
            rows.append((label, entry))

        # Now load the connection data
        self.load_connection_data()

        # add a submit button
        submit_button = customtkinter.CTkButton(master=self.inner_inner_frame,
                                                fg_color=SEC_COLOR,
                                                text="Submit",
                                                font=("Arial", 28, "bold"),
                                                hover_color=FOURTH_COLOR,
                                                text_color="white",
                                                command=lambda: self.submit(controller))  # invoking submit_page1
        submit_button.pack(pady=10)

        # center the input rows within the right container
        for row in rows:
            row[0].pack_configure(anchor="center")
            row[1].pack_configure(anchor="center")

        # Error Message
        self.error_message = tk.Label(self.inner_frame, text="",
                                                font=("Arial", 37, "bold"),
                                                bg=MAIN_COLOR, fg="Red")
        self.error_message.pack(anchor="center", pady=(30, 70))

    ### Submit Page
    def submit(self, c):
        # getting the input elements
        entries = self.get_entries()
        # saving the connection details
        connection = {
            "IP": entries[0].get(),
            "Port": entries[1].get(),
            "Database": entries[2].get(),
            "Username": entries[3].get(),
            "Password": entries[4].get(),
        }

        # Write to data.json
        try:
            with open('data.json', 'r') as file:
                data = json.load(file)
        except (FileNotFoundError, json.JSONDecodeError):
            data = {"Connections": [], "Queries": {}}

        # Replace or set the connection details
        data["Connections"] = [connection]

        with open('data.json', 'w') as file:
            json.dump(data, file)

        # Check for empty values in each entries
        for _, value in connection.items():
            if not value: 
                self.error_message.config(text="All fields are required")
                return
        else:
            self.error_message.config(text="")

        print(connection)
        # checking if the connection is valid
        try:
            global CONNECTION
            CONNECTION = psycopg2.connect(
                database=connection["Database"],
                user=connection["Username"],
                password=connection["Password"],
                host=connection["IP"],
                port=connection["Port"]
            )
            # CONNECTION = PostgresDB(connection["IP"], connection["Port"], connection["Database"],
            #                         connection["Username"], connection["Password"])
            global CONNECTION_NAME
            CONNECTION_NAME = connection["IP"]
            c.show_frame(QueryPage)
        except:
            self.error_message.config(text="Invalid Connection")
            print("Connection Error")

    def get_entries(self):
        return self.entry_widgets  # Return the stored entry widgets
    
    def load_connection_data(self):
        try:
            with open('data.json', 'r') as file:
                data = json.load(file)
                
                # Check if there are any connections stored
                if data.get("Connections"):
                    last_connection = data["Connections"][-1]  # Load the last connection
                    # Populate the entry fields
                    for i, key in enumerate(["IP", "Port", "Database", "Username", "Password"]):
                        self.entry_widgets[i].insert(0, last_connection.get(key, ""))
                else:
                    print("No connections stored yet.")
        except (FileNotFoundError, json.JSONDecodeError):
            print("Data file not found or invalid format. Creating a new one.")


class QueryPage(ttk.Frame):
    def __init__(self, parent, controller):
        super().__init__(parent)
        self.create_widgets(parent, controller)

    def create_widgets(self, parent, controller):
        # create the left container (history panel)
        self.left_container = tk.Frame(self, width=300, height=800, bg=SEC_COLOR)
        self.left_container.pack(side="left", fill="both", expand=True)
        self.left_container.pack_propagate(0)

        # create the right container (query input)
        self.right_container = tk.Frame(self, width=900, height=800, bg=MAIN_COLOR)
        self.right_container.pack(side="right", fill="both", expand=True)
        self.right_container.pack_propagate(0)

        # add a label to the left container
        history_label = tk.Label(self.left_container, text="Previous\nQueries", bg=SEC_COLOR, font=("Arial", 32, "bold"), fg="white")
        history_label.pack(pady=10)

        # Frame for listbox and scrollbar
        listbox_frame = tk.Frame(self.left_container, bg=SEC_COLOR)
        listbox_frame.pack(side="left", fill="both", expand=True)

        # create a scrollable listbox widget
        self.history_listbox = tk.Listbox(listbox_frame, bg=SEC_COLOR, font=("Arial", 20), fg=FONT_COLOR, selectmode="single", highlightthickness=0, borderwidth=0)
        self.history_listbox.pack(side="left", fill="both", expand=True)

        # create a scrollbar widget and link it to the listbox
        scrollbar = ttk.Scrollbar(listbox_frame, orient="vertical", command=self.history_listbox.yview)
        scrollbar.pack(side="right", fill="y")
        self.history_listbox.configure(yscrollcommand=scrollbar.set)
        self.history_listbox.bind("<<ListboxSelect>>", self.handle_click_history)

        # create the right inner container
        self.right_inner_container = tk.Frame(self.right_container, width=800, height=800, bg=MAIN_COLOR)
        self.right_inner_container.pack(anchor="center")
        self.right_inner_container.pack_propagate(0)

        # Header for Enter SQL Query
        input_label = tk.Label(self.right_inner_container, text="Enter SQL Query Statement", font=("Arial", 28, "bold"),
                               bg=MAIN_COLOR, fg="white")
        input_label.pack(pady=20)

        # Query Input Box
        self.query_textbox = customtkinter.CTkTextbox(self.right_inner_container,
                                              width=800, height=245,
                                              fg_color="white",  # Background color of the textbox
                                              text_color="black",  # Text (font) color
                                              font=("Courier", 20, "normal"))
        self.query_textbox.pack(padx=10, pady=10)

        # Return to Connection Page Button
        database_back_button = customtkinter.CTkButton(master=self.right_inner_container,
                                                       fg_color=SEC_COLOR,
                                                       text="Back to Connection",
                                                       font=("Arial", 28, "bold"),
                                                       hover_color=FOURTH_COLOR,
                                                       text_color="white",
                                                       command=lambda: controller.show_frame(ConnectionPage))
        database_back_button.pack(side="left", anchor="s", pady=(0, 35))

        # Execute Query button
        execute_button = customtkinter.CTkButton(master=self.right_inner_container,
                                                 fg_color=SEC_COLOR,
                                                 text="Execute",
                                                 font=("Arial", 28, "bold"),
                                                 hover_color=FOURTH_COLOR,
                                                 text_color="white",
                                                 command=lambda: self.execute_query())  # invoking the validation function
        execute_button.pack(side="right", anchor="s", pady=(0, 35))

        # ttk.Label(self, text="Enter SQL Query").grid(row=0, column=0, padx=10, pady=10)
        
        # # Text widget is used here as ttk doesn't have a direct equivalent of CTkTextbox
        # self.query_text = tk.Text(self, height=10, width=40)
        # self.query_text.grid(row=1, column=0, padx=10, pady=10)
        
        # submit_button = ttk.Button(self, text="Execute Query", command=self.execute_query)
        # submit_button.grid(row=2, column=0, padx=10, pady=10)

    def execute_query(self):
        query = self.query_textbox.get("1.0", "end-1c")
        print(query)
        # Add logic to execute the query

    def load_query_history(self):
        try:
            with open('data.json', 'r') as f:
                d = json.load(f)
                queries = d['Queries']
                if queries != {}:
                    current_queries = queries[CONNECTION_NAME]
                    for i in range(len(current_queries)):
                        self.history_listbox.insert("end", "   " + f"{current_queries[i][0]}")
                        if i % 2 == 0:
                            self.history_listbox.itemconfig(i, {'bg': THIRD_COLOR})
                        else:
                            self.history_listbox.itemconfig(i, {'bg': FOURTH_COLOR})
        except:
            print('Loading Query History Error')

    def handle_click_history(self, event):
        selection = event.widget.curselection()
        if selection:
            index = selection[0]
            name = event.widget.get(index)[3:]
            x = None
            # loading the data from the json file
            with open('data.json', 'r') as _:
                x = json.load(_)
            for t in x["Queries"][CONNECTION_NAME]:
                if t[0] == name:
                    entries = self.get_entries()
                    for a in range(3):
                        # loading the name of the comparison and the two queries
                        if a == 0:
                            entries[a].delete(0, tk.END)
                            entries[a].insert(0, t[a])
                        else:
                            entries[a].delete(1.0, tk.END)
                            entries[a].insert(1.0, t[a])
    
    def get_entries(self):
        return self.entry_widgets  # Return the stored entry widgets


app = MainApplication()
app.mainloop()

### DEMO BELOW ###############
# root = customtkinter.CTk()
# root.geometry("500x350")

# def login(a, b):
#     print(a.get())
#     print(b.get())

# frame = customtkinter.CTkFrame(master=root)
# frame.pack(pady=20, padx=60, fill="both", expand=True)

# label = customtkinter.CTkLabel(master=frame, text="Login System")
# label.pack(pady=12, padx=10)

# entry1 = customtkinter.CTkEntry(master=frame, placeholder_text="Username")
# entry1.pack(pady=12, padx=10)

# entry2 = customtkinter.CTkEntry(master=frame, placeholder_text="Password")
# entry2.pack(pady=12, padx=10)

# button = customtkinter.CTkButton(master=frame, text="Login", command=lambda: login(entry1, entry2))
# button.pack(pady=12, padx=10)

# root.mainloop()

